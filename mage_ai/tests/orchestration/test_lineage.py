"""
OpenLineage events of MageML's runs (orchestration/lineage.py): a scheduled pipeline run and
its block runs reach the backend in the OpenLineage spec's form (validated against its JSON
schemas, in integration_tests/lineage/schemas), with the blocks' outputs as datasets.
"""
import json
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import lineage
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase

SCHEMAS = Path(__file__).resolve().parents[3] / 'integration_tests' / 'lineage' / 'schemas'
SCHEMA_URLS = {
    'OpenLineage-2-0-2.json': 'https://openlineage.io/spec/2-0-2/OpenLineage.json',
    'ParentRunFacet-1-1-0.json': 'https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json',
    'SchemaDatasetFacet-1-1-1.json':
        'https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json',
    'ErrorMessageRunFacet-1-0-1.json':
        'https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json',
}


def validate(event: dict) -> None:
    registry = Registry().with_resources([
        (url, Resource.from_contents(json.loads((SCHEMAS / name).read_text())))
        for name, url in SCHEMA_URLS.items()
    ])
    facets = list(event['run'].get('facets', {}).values()) + [
        facet for dataset in event['inputs'] + event['outputs']
        for facet in dataset.get('facets', {}).values()
    ]
    for value, url in [(event, event['schemaURL'])] + [(f, f['_schemaURL']) for f in facets]:
        Draft202012Validator({'$ref': url}, registry=registry).validate(value)


class LineageTest(DBTestCase):
    def setUp(self):
        super().setUp()
        events = self.events = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                events.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(201)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        environment = patch.dict('os.environ', {
            'OPENLINEAGE_URL': f'http://127.0.0.1:{server.server_address[1]}',
            'OPENLINEAGE_NAMESPACE': 'shop',
        })
        environment.start()
        self.addCleanup(environment.stop)
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def block(self, name, kind, code, upstream=()):
        block = Block.create(f'{self.pipeline.uuid}_{name}', kind, self.repo_path,
                             language='python')
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def run_pipeline(self):
        run = trigger_pipeline(self.pipeline.uuid)
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        try:
            PipelineExecutor(self.pipeline, execution_partition=run.execution_partition).execute(
                pipeline_run_id=run.id, global_vars=run.get_variables(),
            )
        except Exception:
            # In this process a failed block raises; the scheduler records the failure.
            pass
        scheduler.schedule()
        run.refresh()
        lineage.flush()
        return run

    def test_a_runs_events_follow_the_openlineage_spec(self):
        load = self.block('load', 'data_loader', '''
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1, 2], 'amount': [10.5, 2.0]})
        ''')
        self.block('total', 'transformer', '''
            @transformer
            def total(orders, *args, **kwargs):
                orders['total'] = orders['amount'] * 2
                return orders
        ''', upstream=[load])

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        for event in self.events:
            validate(event)
        name = self.pipeline.uuid
        by_job = {}
        for event in self.events:
            by_job.setdefault(event['job']['name'], []).append(event['eventType'])
        self.assertEqual(by_job, {
            name: ['START', 'COMPLETE'],
            f'{name}.{name}_load': ['START', 'COMPLETE'],
            f'{name}.{name}_total': ['START', 'COMPLETE'],
        })
        [total] = [e for e in self.events
                   if e['job']['name'] == f'{name}.{name}_total' and e['eventType'] == 'COMPLETE']
        pipeline_run = [e for e in self.events if e['job']['name'] == name][0]['run']['runId']
        self.assertEqual(total['run']['facets']['parent']['run']['runId'], pipeline_run)
        self.assertEqual(total['inputs'], [{'namespace': 'shop', 'name': f'{name}.{name}_load'}])
        columns = [f['name'] for f in total['outputs'][0]['facets']['schema']['fields']]
        self.assertEqual(columns, ['id', 'amount', 'total'])

    def test_a_failed_block_sends_fail_with_its_error(self):
        self.block('broken', 'data_loader', '''
            @data_loader
            def load(*args, **kwargs):
                raise ValueError('the source is gone')
        ''')

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.FAILED)
        for event in self.events:
            validate(event)
        name = self.pipeline.uuid
        [failed] = [e for e in self.events if e['eventType'] == 'FAIL'
                    and e['job']['name'] == f'{name}.{name}_broken']
        self.assertIn('the source is gone', failed['run']['facets']['errorMessage']['message'])
        self.assertEqual(
            [e['eventType'] for e in self.events if e['job']['name'] == name], ['START', 'FAIL'])
