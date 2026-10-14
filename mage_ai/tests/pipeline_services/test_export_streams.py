"""
mage export service with streaming pipelines: the YAML source and sinks become the stream's
connectors in the manifest, secrets stay references, and what services cannot run yet stops
the export with a reason.
"""
import json
import os
import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.constants import PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.pipeline_services import export
from mage_ai.tests.pipeline_services.test_export import SERVICE_BINARY, ExportTestCase

SOURCE = '''
    connector_type: kafka
    bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
    topic: orders
    consumer_group: orders-clean
    auto_offset_reset: earliest
    batch_size: 250
    timeout_ms: 200
    security_protocol: SASL_SSL
    sasl_config:
      mechanism: PLAIN
      username: "{{ env_var('KAFKA_USER') }}"
      password: "{{ env_var('KAFKA_PASSWORD') }}"
'''
CLEAN = '''
    from typing import Dict, List

    @transformer
    def clean(messages: List[Dict], *args, **kwargs):
        return [m for m in messages if m.get('amount') is not None]
'''
SINK = '''
    connector_type: kafka
    bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
    topic: orders_clean
'''
PRINT = '''
    connector_type: dummy
    print_msg: true
'''


class StreamExportTestCase(ExportTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(
            f'{self._testMethodName}_stream', pipeline_type=PipelineType.STREAMING,
            repo_path=self.repo_path,
        )
        self.addCleanup(self.pipeline.delete)

    def add(self, name, kind, code, upstream=(), language='python', **kwargs):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language=language,
            pipeline=self.pipeline, **kwargs,
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def stream(self, source=SOURCE, transformer=CLEAN, sink=SINK, source_language='yaml'):
        source_block = self.add('source', 'data_loader', source, language=source_language)
        upstream = source_block
        if transformer is not None:
            upstream = self.add('clean', 'transformer', transformer, upstream=[source_block])
        self.add('sink', 'data_exporter', sink, upstream=[upstream], language='yaml')
        return source_block

    def problems(self) -> str:
        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [self.pipeline.uuid])
        return str(caught.exception)


class StreamManifestTest(StreamExportTestCase):
    def test_a_kafka_stream_becomes_the_manifest_stream(self):
        self.stream()
        printer = self.add(
            'print', 'data_exporter', PRINT, upstream=[self.pipeline.get_block(
                f'{self.pipeline.uuid}_clean')], language='yaml',
        )

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        [pipeline] = captured.manifest['pipelines']
        uuid = self.pipeline.uuid
        self.assertEqual(pipeline['triggers'], [])
        self.assertEqual([b['uuid'] for b in pipeline['blocks']], [f'{uuid}_clean'])
        self.assertEqual(pipeline['blocks'][0]['upstream'], [])
        stream = pipeline['stream']
        self.assertEqual(stream['batch'], dict(max_records=250, max_wait_ms=200))
        self.assertEqual(stream['source']['name'], f'{uuid}_source')
        self.assertEqual(stream['source']['kind'], 'kafka')
        source = stream['source']['config']
        self.assertEqual(source['topic'], 'orders')
        self.assertEqual(source['consumer_group'], 'orders-clean')
        self.assertNotIn('batch_size', source)
        self.assertNotIn('connector_type', source)
        self.assertEqual(
            source['sasl_config']['password'], {'$parts': [{'env': 'KAFKA_PASSWORD'}]},
        )
        sinks = {s['name']: s for s in stream['sinks']}
        self.assertEqual(sinks[f'{uuid}_sink']['after'], f'{uuid}_clean')
        self.assertEqual(sinks[f'{uuid}_sink']['config']['topic'], 'orders_clean')
        self.assertEqual(sinks[printer.uuid], dict(
            name=printer.uuid, kind='stdout', config={}, after=f'{uuid}_clean',
        ))
        self.assertTrue(captured.needs_kafka)

    def test_the_app_summary_shows_what_the_stream_reads_and_writes(self):
        from mage_ai.pipeline_services.exports import summary

        self.stream()

        [pipeline] = summary(export.capture(self.repo_path, [self.pipeline.uuid]), None)[
            'pipelines']

        uuid = self.pipeline.uuid
        self.assertEqual(pipeline['stream'], dict(
            source=dict(after=None, kind='kafka', name=f'{uuid}_source', topics=['orders']),
            sinks=[dict(after=f'{uuid}_clean', kind='kafka', name=f'{uuid}_sink',
                        topics=['orders_clean'])],
        ))

    def test_connector_variables_never_reach_the_blocks(self):
        """used_by decides what reaches block processes; connectors are listed apart."""
        self.stream()

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        variables = {v['name']: v for v in captured.manifest['environment']}
        password = variables['KAFKA_PASSWORD']
        self.assertEqual(password['used_by'], [])
        self.assertTrue(password['secret'])
        self.assertTrue(password['required'])
        self.assertEqual(sorted(variables['KAFKA_BOOTSTRAP']['connectors']), [
            f'{self.pipeline.uuid}_sink', f'{self.pipeline.uuid}_source',
        ])

    def test_the_yaml_files_count_in_the_digest_but_are_not_copied(self):
        self.stream()
        first = export.capture(self.repo_path, [self.pipeline.uuid])
        source = Path(self.pipeline.get_block(f'{self.pipeline.uuid}_source').file_path)
        source.write_text(source.read_text().replace('orders-clean', 'orders-clean-2'))

        second = export.capture(self.repo_path, [self.pipeline.uuid])

        self.assertNotIn(source.resolve(), second.files)
        self.assertNotEqual(
            first.manifest['service']['source_sha256'],
            second.manifest['service']['source_sha256'],
        )

    def test_a_stream_without_transformers_writes_the_source_messages(self):
        self.stream(transformer=None)

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        [pipeline] = captured.manifest['pipelines']
        self.assertEqual(pipeline['blocks'], [])
        self.assertNotIn('after', pipeline['stream']['sinks'][0])

    def test_the_image_builds_the_engine_with_kafka(self):
        self.stream()
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        export.write(export.capture(self.repo_path, [self.pipeline.uuid]), str(out), force=True)

        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn('--features mage-service/kafka', dockerfile)
        self.assertIn('cmake g++ make', dockerfile)
        # librdkafka links statically: the runtime stage installs no library for it.
        runtime = dockerfile.split('AS build', 1)[1].split('\nFROM ', 1)[1]
        self.assertNotIn('libcurl', runtime)
        manifest = json.loads((out / 'service.json').read_text())
        self.assertEqual(manifest['pipelines'][0]['stream']['source']['kind'], 'kafka')

    def test_a_stream_service_installs_only_what_its_blocks_import(self):
        """
        Stream blocks pass JSON messages: pandas, pyarrow and NumPy (about 300 MB of the image)
        go in only when a block imports them.
        """
        self.stream()
        plain = export.capture(self.repo_path, [self.pipeline.uuid]).requirements
        self.assertFalse({'pandas', 'pyarrow', 'numpy'} & set(plain), plain)

        clean = Path(self.pipeline.get_block(f'{self.pipeline.uuid}_clean').file_path)
        clean.write_text('import pandas as pd\n' + clean.read_text())
        with_pandas = export.capture(self.repo_path, [self.pipeline.uuid]).requirements
        self.assertIn('pandas', with_pandas)

    def test_the_engine_accepts_the_exported_stream(self):
        if not SERVICE_BINARY.is_file():
            self.skipTest('needs the mage-service binary (cargo build)')
        self.stream()
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(export.capture(self.repo_path, [self.pipeline.uuid]), str(out), force=True)

        result = subprocess.run(
            [str(SERVICE_BINARY), 'validate'], capture_output=True, text=True, timeout=60,
            env=dict(os.environ, MAGE_SERVICE_DIR=str(out)),
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class WebhookExportTest(StreamExportTestCase):
    def test_a_webhook_source_becomes_the_engines_http_source(self):
        self.stream(source='''
            connector_type: webhook
            answer_within_seconds: 10
            signature:
              header: X-Hub-Signature-256
              prefix: "sha256="
              secret: "{{ env_var('WEBHOOK_SECRET') }}"
        ''')
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        export.write(captured, str(out), force=True)

        source = captured.manifest['pipelines'][0]['stream']['source']
        self.assertEqual(source['kind'], 'http')
        self.assertEqual(source['config']['signature']['secret'],
                         {'$parts': [{'env': 'WEBHOOK_SECRET'}]})
        secret = next(v for v in captured.manifest['environment'] if v['name'] == 'WEBHOOK_SECRET')
        self.assertTrue(secret['secret'])
        self.assertEqual(secret['used_by'], [])
        if SERVICE_BINARY.is_file():
            result = subprocess.run(
                [str(SERVICE_BINARY), 'validate'], capture_output=True, text=True, timeout=60,
                env=dict(os.environ, MAGE_SERVICE_DIR=str(out)),
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_signature_secret_in_the_file_is_refused(self):
        self.stream(source='connector_type: webhook\nsignature:\n  secret: shh\n')
        self.assertIn('signature.secret is written in the file', self.problems())


class HttpSinkExportTest(StreamExportTestCase):
    def test_an_http_sink_exports_with_its_headers_as_references(self):
        self.stream(sink='''
            connector_type: http
            url: https://hooks.example.com/orders
            format: ndjson
            headers:
              Authorization: "Bearer {{ env_var('HOOK_TOKEN') }}"
              X-Source: mageml
        ''')

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        [sink] = captured.manifest['pipelines'][0]['stream']['sinks']
        self.assertEqual(sink['kind'], 'http')
        self.assertEqual(sink['config']['headers']['Authorization'],
                         {'$parts': ['Bearer ', {'env': 'HOOK_TOKEN'}]})
        self.assertEqual(sink['config']['headers']['X-Source'], 'mageml')
        token = next(v for v in captured.manifest['environment'] if v['name'] == 'HOOK_TOKEN')
        self.assertEqual(token['used_by'], [])

    def test_a_credential_header_written_in_the_file_is_refused(self):
        self.stream(sink='''
            connector_type: http
            url: https://hooks.example.com/orders
            headers:
              Authorization: Bearer abc123
        ''')
        self.assertIn('the Authorization header is written in the file', self.problems())


class FeastSinkExportTest(StreamExportTestCase):
    def test_a_feast_sink_exports_its_push_source(self):
        self.stream(sink='''
            connector_type: feast
            url: http://feast:6566
            push_source_name: customer_features_push
            to: online_and_offline
        ''')

        [sink] = export.capture(self.repo_path, [self.pipeline.uuid]).manifest[
            'pipelines'][0]['stream']['sinks']

        self.assertEqual(sink['kind'], 'feast')
        self.assertEqual(sink['config'], dict(
            url='http://feast:6566', push_source_name='customer_features_push',
            to='online_and_offline',
        ))

    def test_a_feast_sink_needs_its_push_source(self):
        self.stream(sink='connector_type: feast\nurl: http://feast:6566\n')
        self.assertIn('a Feast sink needs push_source_name', self.problems())


class StreamRefusalTest(StreamExportTestCase):
    def test_unsupported_connectors_name_what_runs_now(self):
        self.stream(source='connector_type: rabbitmq\nconnection_host: localhost\n')
        problems = self.problems()
        self.assertIn(
            'uses the rabbitmq connector; services run kafka, webhook sources now', problems,
        )

    def test_python_sources_are_refused(self):
        self.stream(source='''
            from mage_ai.streaming.sources.base_python import BasePythonSource

            @streaming_source
            class Source(BasePythonSource):
                def batch_read(self, handler):
                    handler([{'a': 1}])
        ''', source_language='python')
        self.assertIn('Custom Python sources come later', self.problems())

    def test_a_password_written_in_the_file_is_refused(self):
        self.stream(source=SOURCE.replace(
            "\"{{ env_var('KAFKA_PASSWORD') }}\"", 'hunter2',  # pragma: allowlist secret
        ))
        self.assertIn('sasl_config.password is written in the file', self.problems())

    def test_settings_services_cannot_honour_are_refused(self):
        self.stream(source=textwrap.dedent(SOURCE) + textwrap.dedent('''
            include_metadata: true
            serde_config:
              serialization_method: AVRO
            offset:
              type: beginning
            fetch_size: 10
        '''))
        problems = self.problems()
        self.assertIn('include_metadata is not supported', problems)
        self.assertIn('AVRO messages are not supported', problems)
        self.assertIn('offset (a start position) is not supported', problems)
        self.assertIn('unknown Kafka settings fetch_size', problems)

    def test_a_sink_on_the_source_next_to_transformers_is_refused(self):
        source = self.stream()
        self.add('raw', 'data_exporter', PRINT, upstream=[source], language='yaml')
        self.assertIn("writes the source's messages next to transformers", self.problems())

    def test_secret_managers_are_refused_in_connector_settings(self):
        self.stream(sink=SINK + "sasl_config:\n  password: \"{{ aws_secret_var('k') }}\"\n")
        self.assertIn('aws_secret_var()', self.problems())

    def test_invalid_yaml_is_a_problem_not_a_crash(self):
        self.stream(source='connector_type: kafka\ntopic: [orders\n')
        self.assertIn('its YAML is not valid', self.problems())

    def test_variables_too_large_for_64_bits_are_refused(self):
        self.stream()
        self.pipeline.variables = dict(customer=123456789012345678901234567890, fine=2**63)
        self.pipeline.save()

        problems = self.problems()

        self.assertIn('variable customer: 123456789012345678901234567890 is too large', problems)
        self.assertNotIn('variable fine', problems)
