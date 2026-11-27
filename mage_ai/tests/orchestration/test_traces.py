"""
Traces of MageML's runs (orchestration/traces.py): a scheduled run is a span, each block
attempt a span under it sent from the attempt's process, and block code sees its attempt's
span as TRACEPARENT.
"""
import json
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import traces
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase


class ConfigTest(TestCase):
    def test_the_servers_grpc_endpoint_is_not_taken_for_http(self):
        with patch.dict('os.environ', {'OTEL_EXPORTER_OTLP_ENDPOINT': 'http://c:4317'},
                        clear=True):
            self.assertIsNone(traces.config())
        with patch.dict('os.environ', {'OTEL_EXPORTER_OTLP_ENDPOINT': 'http://c:4318/',
                                       'OTEL_EXPORTER_OTLP_PROTOCOL': 'http/protobuf'},
                        clear=True):
            self.assertEqual(traces.config()['endpoint'], 'http://c:4318/v1/traces')
        with patch.dict('os.environ', {
            'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT': 'http://c:4318/v1/traces',
            'OTEL_EXPORTER_OTLP_HEADERS': 'authorization=Bearer%20k',
            'OTEL_SERVICE_NAME': 'runs',
        }, clear=True):
            settings = traces.config()
            self.assertEqual(settings['headers'], {'authorization': 'Bearer k'})
            self.assertEqual(settings['resource']['service.name'], 'runs')

    def test_a_runs_ids_are_the_same_in_every_process_and_sampling_follows_them(self):
        self.assertEqual(traces.run_ids('shop', 7), traces.run_ids('shop', 7))
        self.assertNotEqual(traces.run_ids('shop', 7), traces.run_ids('shop', 8))
        trace, span = traces.run_ids('shop', 7)
        self.assertEqual((len(trace), len(span)), (32, 16))
        kept = sum(traces.sampled(traces.run_ids('shop', n)[0], dict(ratio=0.25))
                   for n in range(2000))
        self.assertTrue(350 < kept < 650, kept)


class RunTracesTest(DBTestCase):
    def setUp(self):
        super().setUp()
        spans = self.spans = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                spans.extend(body['resourceSpans'][0]['scopeSpans'][0]['spans'])
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        environment = patch.dict('os.environ', {
            'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT':
                f'http://127.0.0.1:{server.server_address[1]}/v1/traces',
        })
        environment.start()
        self.addCleanup(environment.stop)
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.seen = Path(self.repo_path, f'{self._testMethodName}.traceparent')

    def test_a_run_and_its_blocks_are_one_trace_that_block_code_continues(self):
        load = Block.create(f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path,
                            language='python')
        Path(load.file_path).write_text(textwrap.dedent(f'''
            import os

            @data_loader
            def load(*args, **kwargs):
                with open({str(self.seen)!r}, 'w') as file:
                    file.write(os.environ.get('TRACEPARENT', 'none'))
                return 1
        '''))
        self.pipeline.add_block(load)
        run = trigger_pipeline(self.pipeline.uuid)
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        PipelineExecutor(self.pipeline, execution_partition=run.execution_partition).execute(
            pipeline_run_id=run.id, global_vars=run.get_variables(),
        )
        scheduler.schedule()
        run.refresh()
        traces.flush()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        by_name = {s['name']: s for s in self.spans}
        run_span = by_name[f'run {self.pipeline.uuid}']
        block_span = by_name[f'block {load.uuid}']
        self.assertEqual(block_span['traceId'], run_span['traceId'])
        self.assertEqual(block_span['parentSpanId'], run_span['spanId'])
        self.assertNotIn('parentSpanId', run_span)
        self.assertLessEqual(int(run_span['startTimeUnixNano']),
                             int(block_span['startTimeUnixNano']))
        self.assertEqual(
            self.seen.read_text(),
            f"00-{block_span['traceId']}-{block_span['spanId']}-01",
        )
        # Not left set for whatever runs next in the process.
        import os

        self.assertNotIn('TRACEPARENT', os.environ)
