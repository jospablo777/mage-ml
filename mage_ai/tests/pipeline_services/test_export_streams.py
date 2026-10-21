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

    def test_secret_fields_are_for_sample_capture_and_not_a_connector_setting(self):
        self.stream(source=SOURCE + '    secret_fields: [customer.email]\n')

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        source = captured.manifest['pipelines'][0]['stream']['source']['config']
        self.assertNotIn('secret_fields', source)
        self.assertEqual(source['topic'], 'orders')

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

    def test_an_amplitude_source_dedupes_by_the_events_uuid(self):
        self.stream(source='connector_type: amplitude\n')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        source = captured.manifest['pipelines'][0]['stream']['source']
        self.assertEqual(source['kind'], 'http')
        self.assertEqual(source['config'], {'idempotency_key_field': 'uuid'})

    def test_a_webhook_key_field_must_name_a_field(self):
        self.stream(source='connector_type: webhook\nidempotency_key_field: ""\n')
        self.assertIn('idempotency_key_field must name a field', self.problems())

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


class AmplitudeSinkExportTest(StreamExportTestCase):
    def test_an_amplitude_sink_exports_its_key_from_the_environment(self):
        self.stream(sink='''
            connector_type: amplitude
            api_key: "{{ env_var('AMPLITUDE_API_KEY') }}"
            region: eu
            event_type: Order Completed
            columns:
              user_id: customer_id
              time: ordered_at
            key_columns: [order_id]
        ''')

        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        [sink] = captured.manifest['pipelines'][0]['stream']['sinks']

        self.assertEqual(sink['kind'], 'amplitude')
        self.assertEqual(sink['config'], dict(
            api_key={'$parts': [{'env': 'AMPLITUDE_API_KEY'}]}, region='EU',
            event_type='Order Completed',
            columns=dict(user_id='customer_id', time='ordered_at'), key_columns=['order_id'],
        ))
        self.assertIn('AMPLITUDE_API_KEY', [v['name'] for v in captured.manifest['environment']])

    def test_an_amplitude_sink_refuses_a_written_key_and_unknown_columns(self):
        self.stream(sink='''
            connector_type: amplitude
            api_key: abc123
            region: APAC
            columns:
              email: customer_email
        ''')
        problems = self.problems()
        self.assertIn('api_key is written in the file', problems)
        self.assertIn('region is US or EU', problems)
        self.assertIn('columns maps user_id, device_id, time or insert_id', problems)


class DeadLetterExportTest(StreamExportTestCase):
    DEAD = '''
        connector_type: kafka
        bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
        topic: orders_dead
        dead_letter: true
    '''

    def test_a_sink_marked_dead_letter_becomes_the_streams_dead_letter_sink(self):
        source = self.stream()
        dead = self.add('dead', 'data_exporter', self.DEAD, upstream=[source], language='yaml')

        stream = export.capture(self.repo_path, [self.pipeline.uuid]).manifest[
            'pipelines'][0]['stream']

        self.assertEqual(stream['errors']['dead_letter']['name'], dead.uuid)
        self.assertEqual(stream['errors']['dead_letter']['config']['topic'], 'orders_dead')
        self.assertNotIn('after', stream['errors']['dead_letter'])
        self.assertNotIn('dead_letter', stream['errors']['dead_letter']['config'])
        self.assertEqual([s['name'] for s in stream['sinks']], [f'{self.pipeline.uuid}_sink'])

    def test_a_stream_has_one_dead_letter_sink(self):
        source = self.stream()
        self.add('dead', 'data_exporter', self.DEAD, upstream=[source], language='yaml')
        self.add('dead2', 'data_exporter', self.DEAD, upstream=[source], language='yaml')
        self.assertIn('has 2 dead-letter sinks', self.problems())


class EventTimeExportTest(StreamExportTestCase):
    def source_with(self, event_time: str) -> str:
        return (textwrap.dedent(SOURCE).strip() + '\n'
                + textwrap.dedent(event_time).strip() + '\n')

    def test_the_sources_event_time_becomes_the_streams_time(self):
        source = self.stream(source=self.source_with('''
            event_time:
              field: created_at
              format: rfc3339
              lateness_seconds: 2.5
        '''))
        self.add('dead', 'data_exporter', DeadLetterExportTest.DEAD, upstream=[source],
                 language='yaml')

        stream = export.capture(self.repo_path, [self.pipeline.uuid]).manifest[
            'pipelines'][0]['stream']

        self.assertEqual(stream['time'], dict(field='created_at', format='rfc3339',
                                              lateness_ms=2500, on_late='dead_letter'))
        self.assertNotIn('event_time', stream['source']['config'])

    def test_event_time_says_what_to_fix(self):
        self.stream(source=self.source_with('''
            event_time:
              format: iso
              lateness_seconds: -1
        '''))
        problems = self.problems()
        self.assertIn('format is one of epoch_ms, epoch_s, rfc3339', problems)
        self.assertIn('lateness_seconds is a number of seconds', problems)
        self.assertIn('late messages go to the dead-letter sink, and the stream has none',
                      problems)

    def test_with_pass_no_dead_letter_sink_is_needed(self):
        self.stream(source=self.source_with('''
            event_time: {on_late: pass}
        '''))
        stream = export.capture(self.repo_path, [self.pipeline.uuid]).manifest[
            'pipelines'][0]['stream']
        self.assertEqual(stream['time'], dict(format='epoch_ms', on_late='pass', lateness_ms=0))


class RabbitmqExportTest(StreamExportTestCase):
    SOURCE = '''
        connector_type: rabbitmq
        connection_host: rabbit
        connection_port: 5672
        queue_name: orders
        username: svc
        password: "{{ env_var('RABBIT_PASSWORD') }}"
        batch_size: 50
        batch_timeout: 0.5
    '''
    SINK = '''
        connector_type: rabbitmq
        connection_host: rabbit
        connection_port: 5672
        queue_name: orders_clean
        password: "{{ env_var('RABBIT_PASSWORD') }}"
    '''

    def test_rabbitmq_source_and_sink_export_with_their_engine_feature(self):
        self.stream(source=self.SOURCE, sink=self.SINK)
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        export.write(captured, str(out), force=True)

        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'rabbitmq')
        self.assertEqual(stream['source']['config']['queue_name'], 'orders')
        self.assertEqual(stream['batch'], dict(max_records=50, max_wait_ms=500))
        self.assertEqual(stream['sinks'][0]['kind'], 'rabbitmq')
        self.assertEqual(captured.engine_features, ['rabbitmq'])
        self.assertIn('--features mage-service/rabbitmq', (out / 'Dockerfile').read_text())
        password = next(v for v in captured.manifest['environment']
                        if v['name'] == 'RABBIT_PASSWORD')
        self.assertTrue(password['secret'])
        self.assertEqual(password['used_by'], [])

    def test_auto_ack_and_written_passwords_are_refused(self):
        self.stream(source=textwrap.dedent(self.SOURCE).replace(
            "\"{{ env_var('RABBIT_PASSWORD') }}\"", 'hunter2',  # pragma: allowlist secret
        ) + 'consume_config:\n  auto_ack: true\n')
        problems = self.problems()
        self.assertIn('password is written in the file', problems)
        self.assertIn('auto_ack', problems)


class IbmMqExportTest(StreamExportTestCase):
    SOURCE = '''
        connector_type: ibm_mq
        queue_manager: QM1
        channel: APP.SVRCONN
        connection_name: "mq1.example.com(1414),mq2.example.com(1414)"
        queue: ORDERS.IN
        user: app
        password: "{{ env_var('MQ_PASSWORD') }}"
        connections: 2
        batch_size: 100
        batch_timeout: 0.25
    '''
    SINK = '''
        connector_type: ibm_mq
        queue_manager: QM1
        channel: APP.SVRCONN
        host: mq1.example.com
        queue: ORDERS.CLEAN
        user: app
        password: "{{ env_var('MQ_PASSWORD') }}"
        cipher_spec: ANY_TLS13_OR_HIGHER
        key_repository: /etc/mq/keys/client
    '''

    def test_ibm_mq_exports_with_its_feature_and_the_pinned_client_in_the_image(self):
        self.stream(source=self.SOURCE, sink=self.SINK)
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        export.write(captured, str(out), force=True)

        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'ibm_mq')
        self.assertEqual(stream['source']['config']['connections'], 2)
        self.assertEqual(stream['source']['config']['queue'], 'ORDERS.IN')
        self.assertEqual(stream['batch'], dict(max_records=100, max_wait_ms=250))
        self.assertEqual(stream['sinks'][0]['config']['cipher_spec'], 'ANY_TLS13_OR_HIGHER')
        self.assertEqual(captured.engine_features, ['ibm-mq'])
        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn('--features mage-service/ibm-mq', dockerfile)
        self.assertIn(f'{export.IBM_MQ_CLIENT_SHA256}  /tmp/mq.tar.gz" | sha256sum -c -',
                      dockerfile)
        self.assertIn('build with --platform linux/amd64', dockerfile)
        # IBM's server refuses connections now and then; a build tries again.
        self.assertIn('curl -fsSL --retry 5 --retry-all-errors', dockerfile)
        self.assertIn('COPY --from=ibm-mq /opt/mqm /opt/mqm', dockerfile)
        self.assertIn('MAGE_SERVICE_MQ_LIBRARY=/opt/mqm/lib64/libmqm_r.so', dockerfile)
        self.assertIn('MQ_OVERRIDE_DATA_PATH=/tmp/mage-mq', dockerfile)
        self.assertIn("-name en_US -exec rm -rf {} +", dockerfile)
        password = next(v for v in captured.manifest['environment']
                        if v['name'] == 'MQ_PASSWORD')
        self.assertTrue(password['secret'])

    def test_streams_without_ibm_mq_carry_no_mq_client(self):
        self.stream(source=SOURCE, sink=SINK)
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        export.write(captured, str(out), force=True)
        self.assertNotIn('/opt/mqm', (out / 'Dockerfile').read_text())

    def test_missing_and_unsafe_settings_are_refused(self):
        source = textwrap.dedent(self.SOURCE).replace('channel: APP.SVRCONN\n', '').replace(
            "\"{{ env_var('MQ_PASSWORD') }}\"", 'hunter2',  # pragma: allowlist secret
        ) + 'cipher_spec: ANY_TLS13\nreconnect: true\n'
        self.stream(source=source)
        problems = self.problems()
        self.assertIn('IBM MQ settings need channel', problems)
        self.assertIn('password is written in the file', problems)
        self.assertIn('IBM MQ TLS needs both cipher_spec and key_repository', problems)
        self.assertIn('unknown IBM MQ settings reconnect', problems)


class ObjectStorageExportTest(StreamExportTestCase):
    def test_mage_s3_sinks_export_with_their_buffers_as_batches(self):
        self.stream(sink='''
            connector_type: amazon_s3
            bucket: lake
            prefix: /orders/raw/
            file_type: parquet
            buffer_size_mb: 16
            buffer_timeout_seconds: 60
            date_partition_format: "%Y/%m/%d"
        ''')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['sinks'][0]['kind'], 'object_storage')
        self.assertEqual(stream['sinks'][0]['config'], dict(
            url='s3://lake/orders/raw', format='parquet', date_partition_format='%Y/%m/%d',
        ))
        self.assertEqual(stream['batch']['max_bytes'], 16 * 1024 * 1024)
        self.assertEqual(stream['batch']['max_wait_ms'], 60_000)
        self.assertIn('object-store', captured.engine_features)
        self.assertTrue(any('each batch becomes one object' in n for n in captured.notes))

    def test_the_generic_sink_takes_any_store(self):
        self.stream(sink='''
            connector_type: object_storage
            url: "abfs://lake@shop.dfs.core.windows.net/orders"
            format: jsonl
        ''')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['sinks'][0]['config']['format'], 'jsonl')
        self.assertNotIn('max_bytes', stream['batch'])

    def test_what_would_change_values_or_needs_local_files_is_refused(self):
        self.stream(sink='''
            connector_type: google_cloud_storage
            bucket: lake
            prefix: raw
            file_type: csv
            path_to_credentials_json_file: ./google_credentials.json
        ''')
        problems = self.problems()
        self.assertIn('CSV has no types', problems)
        self.assertIn('GOOGLE_APPLICATION_CREDENTIALS', problems)


class PostgresSinkExportTest(StreamExportTestCase):
    def test_mage_postgres_sinks_export_to_the_engines_sink(self):
        self.stream(sink='''
            connector_type: postgres
            host: db.example.com
            database: shop
            username: stream
            password: "{{ env_var('PG_PASSWORD') }}"
            schema: raw
            table: orders
            unique_constraints: [order_id]
            unique_conflict_method: UPDATE
            sslmode: verify-full
        ''')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        sink = captured.manifest['pipelines'][0]['stream']['sinks'][0]
        self.assertEqual(sink['kind'], 'postgres')
        self.assertEqual(sink['config']['unique_constraints'], ['order_id'])
        self.assertEqual(sink['config']['password'], {'$parts': [{'env': 'PG_PASSWORD'}]})
        self.assertIn('postgres', captured.engine_features)

    def test_a_written_password_and_an_unknown_method_are_refused(self):
        self.stream(sink='''
            connector_type: postgres
            host: db
            database: shop
            username: stream
            password: hunter2
            table: orders
            unique_conflict_method: MERGE
        ''')  # pragma: allowlist secret
        problems = self.problems()
        self.assertIn('password is written in the file', problems)
        self.assertIn('unique_conflict_method is IGNORE or UPDATE', problems)


class PostgresSourceExportTest(StreamExportTestCase):
    def test_change_capture_sources_export_to_the_engines_source(self):
        self.stream(source=textwrap.dedent('''
            connector_type: postgres
            host: db.example.com
            database: shop
            user: replicator
            password: "{{ env_var('PG_PASSWORD') }}"
            replication_slot: orders_slot
            publication_name: orders_pub
            tables: [public.orders]
            batch_size: 500
            batch_timeout: 2
            start_lsn: 0/16B3748
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        source = stream['source']
        self.assertEqual(source['kind'], 'postgres')
        self.assertEqual(source['config']['replication_slot'], 'orders_slot')
        self.assertEqual(source['config']['tables'], ['public.orders'])
        self.assertEqual(source['config']['password'], {'$parts': [{'env': 'PG_PASSWORD'}]})
        self.assertNotIn('batch_size', source['config'])
        self.assertEqual(stream['batch']['max_records'], 500)
        self.assertEqual(stream['batch']['max_wait_ms'], 2000)
        self.assertIn('postgres', captured.engine_features)

    def test_a_written_password_and_a_bad_start_position_are_refused(self):
        self.stream(source=textwrap.dedent('''
            connector_type: postgres
            host: db
            database: shop
            password: hunter2
            start_lsn: yesterday
            slot: x
        '''))  # pragma: allowlist secret
        problems = self.problems()
        self.assertIn('password is written in the file', problems)
        self.assertIn('start_lsn yesterday is not a log position', problems)
        self.assertIn('PostgreSQL source settings need user', problems)
        self.assertIn('unknown PostgreSQL source settings slot', problems)


class SqsSourceExportTest(StreamExportTestCase):
    def test_sqs_sources_export_to_the_engines_source(self):
        self.stream(source=textwrap.dedent('''
            connector_type: amazon_sqs
            queue_name: orders
            batch_size: 50
            wait_time_seconds: 10
            message_deletion_method: AFTER_RECEIVED
            serde_config:
              serialization_method: RAW_VALUE
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'sqs')
        self.assertEqual(stream['source']['config']['queue_name'], 'orders')
        self.assertNotIn('message_deletion_method', stream['source']['config'])
        self.assertEqual(stream['batch']['max_records'], 50)
        self.assertIn('sqs', captured.engine_features)

    def test_manual_deletion_and_other_formats_are_refused(self):
        self.stream(source=textwrap.dedent('''
            connector_type: amazon_sqs
            message_deletion_method: MANUAL
            serde_config:
              serialization_method: AVRO
        '''))
        problems = self.problems()
        self.assertIn('SQS settings need queue_name or queue_url', problems)
        self.assertIn('message_deletion_method MANUAL', problems)
        self.assertIn('SQS messages are JSON or RAW_VALUE, not AVRO', problems)


class PubSubExportTest(StreamExportTestCase):
    def test_pubsub_streams_export_to_the_engines_source_and_sink(self):
        self.stream(
            source=textwrap.dedent('''
                connector_type: google_cloud_pubsub
                project_id: shop
                topic_id: orders
                subscription_id: scorer
                batch_size: 200
                timeout: 5
            '''),
            sink=textwrap.dedent('''
                connector_type: google_cloud_pubsub
                project_id: shop
                topic_id: scores
            '''),
        )
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'pubsub')
        self.assertEqual(stream['source']['config'], dict(
            project_id='shop', topic_id='orders', subscription_id='scorer',
        ))
        self.assertEqual(stream['batch']['max_records'], 200)
        self.assertEqual(stream['sinks'][0]['kind'], 'pubsub')
        self.assertIn('pubsub', captured.engine_features)

    def test_a_credentials_file_is_refused(self):
        self.stream(sink=textwrap.dedent('''
            connector_type: google_cloud_pubsub
            project_id: shop
            path_to_credentials_json_file: ./google_credentials.json
        '''))
        problems = self.problems()
        self.assertIn('GOOGLE_APPLICATION_CREDENTIALS', problems)
        self.assertIn('Pub/Sub settings need topic_id', problems)


class ActiveMqExportTest(StreamExportTestCase):
    def test_activemq_streams_export_to_the_engines_source_and_sink(self):
        self.stream(
            source=textwrap.dedent('''
                connector_type: activemq
                connection_host: mq.example.com
                connection_port: 61613
                queue_name: orders
                username: reader
                password: "{{ env_var('MQ_PASSWORD') }}"
                batch_size: 100
                batch_timeout: 0.5
            '''),
            sink=textwrap.dedent('''
                connector_type: activemq
                connection_host: mq.example.com
                connection_port: 61613
                queue_name: scores
                receipt_timeout: 10
            '''),
        )
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'activemq')
        self.assertNotIn('batch_size', stream['source']['config'])
        self.assertEqual(stream['batch'], dict(max_records=100, max_wait_ms=500))
        self.assertEqual(stream['sinks'][0]['config']['receipt_timeout'], 10)
        self.assertIn('activemq', captured.engine_features)

    def test_a_written_password_is_refused(self):
        self.stream(sink=textwrap.dedent('''
            connector_type: activemq
            connection_host: mq
            connection_port: 61613
            queue_name: scores
            password: hunter2
        '''))  # pragma: allowlist secret
        self.assertIn('password is written in the file', self.problems())


class SearchSinkExportTest(StreamExportTestCase):
    def export_search(self, connector_type, auth):
        self.stream(sink=textwrap.dedent(f'''
            connector_type: {connector_type}
            host: https://search.example.com
            index_name: orders
            _id: order_id
            {auth}
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        sink = captured.manifest['pipelines'][0]['stream']['sinks'][0]
        self.assertEqual(sink['kind'], 'search')
        self.assertEqual(sink['config']['engine'], connector_type)
        self.assertEqual(sink['config']['_id'], 'order_id')
        self.assertIn('search', captured.engine_features)

    def test_elasticsearch_sinks_export_to_the_search_sink(self):
        self.export_search('elasticsearch', "api_key: \"{{ env_var('ES_KEY') }}\"")

    def test_opensearch_sinks_export_to_the_search_sink(self):
        self.export_search('opensearch', 'http_auth: "@awsauth"')

    def test_written_secrets_and_local_files_are_refused(self):
        self.stream(sink=textwrap.dedent('''
            connector_type: opensearch
            host: https://search.example.com
            index_name: orders
            http_auth: admin:hunter2
            ca_cert: ./ca.pem
            _op_type: upsert
        '''))  # pragma: allowlist secret
        problems = self.problems()
        self.assertIn('http_auth holds a password written in the file', problems)
        self.assertIn('ca_cert names a file on this machine', problems)
        self.assertIn('_op_type is index, create or update', problems)


class InfluxDbSinkExportTest(StreamExportTestCase):
    def test_influxdb_sinks_export_to_the_influxdb_sink(self):
        """Exporting a stream with an InfluxDB sink stopped: the engine had none."""
        self.stream(sink=textwrap.dedent('''
            connector_type: influxdb
            url: https://influx.example.com
            token: "{{ env_var('INFLUX_TOKEN') }}"
            org: analytics
            bucket: events
            measurement: orders
            batch_size: 100
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        sink = captured.manifest['pipelines'][0]['stream']['sinks'][0]
        self.assertEqual(sink['kind'], 'influxdb')
        self.assertEqual(sink['config']['bucket'], 'events')
        self.assertNotIn('batch_size', sink['config'])
        self.assertIn('influxdb', captured.engine_features)

    def test_a_written_token_is_refused(self):
        self.stream(sink=textwrap.dedent('''
            connector_type: influxdb
            url: https://influx.example.com
            token: plain-token-value
            org: analytics
        '''))  # pragma: allowlist secret
        self.assertIn('token is written in the file', self.problems())


class ClickHouseSinkExportTest(StreamExportTestCase):
    def test_the_profile_and_table_become_the_engines_settings(self):
        (Path(self.repo_path) / 'io_config.yaml').write_text(textwrap.dedent('''
            version: 0.1.1
            warehouse:
              CLICKHOUSE_HOST: "{{ env_var('CH_HOST') }}"
              CLICKHOUSE_PORT: 8443
              CLICKHOUSE_INTERFACE: https
              CLICKHOUSE_USERNAME: stream
              CLICKHOUSE_PASSWORD: "{{ env_var('CH_PASSWORD') }}"
              CLICKHOUSE_DATABASE: analytics
        '''))
        self.stream(sink='''
            connector_type: clickhouse
            profile: warehouse
            config:
              table_name: events
        ''')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        sink = captured.manifest['pipelines'][0]['stream']['sinks'][0]
        self.assertEqual(sink['kind'], 'clickhouse')
        self.assertEqual(sink['config']['table'], 'events')
        self.assertEqual(sink['config']['database'], 'analytics')
        self.assertTrue(sink['config']['secure'])
        self.assertEqual(sink['config']['host'], {'$parts': [{'env': 'CH_HOST'}]})
        password = next(v for v in captured.manifest['environment']
                        if v['name'] == 'CH_PASSWORD')
        self.assertTrue(password['secret'])

    def test_a_missing_table_or_profile_is_refused(self):
        self.stream(sink='connector_type: clickhouse\nprofile: nowhere\nconfig: {}\n')
        problems = self.problems()
        self.assertIn('needs config.table_name', problems)
        self.assertIn('no profile nowhere', problems)


class StreamRefusalTest(StreamExportTestCase):
    def test_unsupported_connectors_name_what_runs_now(self):
        self.stream(source='connector_type: kinesis\nstream_name: orders\n')
        problems = self.problems()
        self.assertIn(
            'uses the kinesis connector; services run activemq, amazon_sqs, amplitude, '
            'azure_event_hub, google_cloud_pubsub, ibm_mq, kafka, mongodb, nats, nats_js, '
            'postgres, rabbitmq, webhook '
            'sources now',
            problems,
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
              serialization_method: PROTOBUF
            offset:
              type: beginning
            fetch_size: 10
        '''))
        problems = self.problems()
        self.assertIn('PROTOBUF messages are not supported', problems)
        self.assertIn('offset needs partitions', problems)
        self.assertIn('unknown Kafka settings fetch_size', problems)

    def test_avro_messages_go_with_their_registry_and_its_password_as_a_reference(self):
        self.stream(source=textwrap.dedent(SOURCE) + textwrap.dedent('''
            serde_config:
              serialization_method: AVRO
              schema_registry_url: https://registry.example.com
              schema_registry_username: reader
              schema_registry_password: "{{ env_var('REGISTRY_PASSWORD') }}"
              schema_classpath: unused
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        serde = captured.manifest['pipelines'][0]['stream']['source']['config']['serde_config']
        self.assertEqual(serde['serialization_method'], 'AVRO')
        self.assertEqual(serde['schema_registry_url'], 'https://registry.example.com')
        self.assertNotIn('schema_classpath', serde)
        self.assertIn('REGISTRY_PASSWORD', json.dumps(serde['schema_registry_password']))

    def test_azure_event_hubs_is_read_through_its_kafka_endpoint(self):
        self.stream(source=textwrap.dedent('''
            connector_type: azure_event_hub
            connection_str: "{{ env_var('EVENT_HUBS_CONNECTION') }}"
            eventhub_name: orders
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        source = captured.manifest['pipelines'][0]['stream']['source']
        self.assertEqual(source['kind'], 'kafka')
        self.assertEqual(source['config']['topic'], 'orders')
        self.assertEqual(source['config']['consumer_group'], '$Default')
        self.assertIn('EVENT_HUBS_CONNECTION',
                      json.dumps(source['config']['event_hubs_connection_string']))

    def test_an_event_hubs_connection_string_written_in_the_file_is_refused(self):
        self.stream(source=textwrap.dedent('''
            connector_type: azure_event_hub
            connection_str: Endpoint=sb://x.servicebus.windows.net/;SharedAccessKey=k
        '''))  # pragma: allowlist secret
        problems = self.problems()
        self.assertIn('connection_str is written in the file', problems)
        self.assertIn('Azure Event Hubs needs eventhub_name', problems)

    def test_nats_jetstream_streams_keep_their_settings(self):
        """nats is the name Mage runs; the export refused it and took only nats_js."""
        self.stream(
            source=textwrap.dedent('''
                connector_type: nats
                server_url: nats://nats:4222
                stream_name: orders
                subject: orders.new
                consumer_name: scorer
                batch_size: 200
                timeout: 0.5
                ack_wait: 120
                user_credentials: [reader, "{{ env_var('NATS_PASSWORD') }}"]
            '''),
            sink=textwrap.dedent('''
                connector_type: nats_js
                server_url: nats://nats:4222
                subject: scores.new
            '''),
        )
        # nats_js stays an alias.
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'nats')
        self.assertEqual(stream['source']['config']['consumer_name'], 'scorer')
        self.assertEqual(stream['batch'], dict(stream['batch'], max_records=200, max_wait_ms=500))
        self.assertIn('NATS_PASSWORD', json.dumps(stream['source']['config']))
        self.assertEqual(stream['sinks'][0]['kind'], 'nats')
        self.assertNotIn('stream_name', stream['sinks'][0]['config'])

    def test_a_nats_push_consumer_or_written_password_is_refused(self):
        self.stream(source=textwrap.dedent('''
            connector_type: nats_js
            server_url: nats://nats:4222
            subject: orders.new
            consumer_type: PUSH
            user_credentials: [reader, hunter2]
        '''))  # pragma: allowlist secret
        problems = self.problems()
        self.assertIn('NATS settings need stream_name', problems)
        self.assertIn('pull consumer', problems)
        self.assertIn('NATS password or seed is written in the file', problems)

    def test_event_streams_credentials_stay_a_reference(self):
        self.stream(source=textwrap.dedent('''
            connector_type: kafka
            event_streams_credentials: "{{ env_var('EVENT_STREAMS_CREDENTIALS') }}"
            topic: orders
            consumer_group: scorer
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        config = captured.manifest['pipelines'][0]['stream']['source']['config']
        self.assertIn('EVENT_STREAMS_CREDENTIALS', json.dumps(config['event_streams_credentials']))

    def test_event_streams_credentials_written_in_the_file_are_refused(self):
        self.stream(source=textwrap.dedent('''
            connector_type: kafka
            event_streams_credentials: '{"api_key": "k", "kafka_brokers_sasl": ["b:9093"]}'
            topic: orders
            consumer_group: scorer
        '''))  # pragma: allowlist secret
        self.assertIn('event_streams_credentials is written in the file', self.problems())

    def test_mongodb_streams_keep_their_settings_and_refuse_written_passwords(self):
        self.stream(
            source=textwrap.dedent('''
                connector_type: mongodb
                connection_str: "{{ env_var('MONGO_URL') }}"
                database: shop
                collection: orders
                batch_size: 50
                full_document: updateLookup
            '''),
            sink=textwrap.dedent('''
                connector_type: mongodb
                connection_string: mongodb://mongo:27017
                database_name: shop
                collection_name: states
                unique_constraints: [order]
            '''),
        )
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        self.assertEqual(stream['source']['kind'], 'mongodb')
        self.assertEqual(stream['source']['config']['full_document'], 'updateLookup')
        self.assertEqual(stream['batch']['max_records'], 50)
        self.assertEqual(stream['sinks'][0]['config']['unique_constraints'], ['order'])

    def test_a_mongodb_password_written_in_the_file_is_refused(self):
        self.stream(sink=textwrap.dedent('''
            connector_type: mongodb
            connection_string: mongodb://writer:hunter2@mongo:27017
            database_name: shop
            collection_name: states
        '''))  # pragma: allowlist secret
        self.assertIn('connection_string holds a password written in the file',
                      self.problems())

    def test_a_start_offset_with_partitions_is_kept(self):
        self.stream(source=textwrap.dedent(SOURCE) + textwrap.dedent('''
            offset:
              type: int
              value: 5
              partitions:
                - {topic: orders, partition: 0}
        '''))
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        config = captured.manifest['pipelines'][0]['stream']['source']['config']
        self.assertEqual(config['offset'], dict(type='int', value=5,
                                                partitions=[dict(topic='orders', partition=0)]))

    def test_include_metadata_is_kept_for_the_kafka_source(self):
        self.stream(source=textwrap.dedent(SOURCE) + 'include_metadata: true\n')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        config = captured.manifest['pipelines'][0]['stream']['source']['config']
        self.assertIs(config['include_metadata'], True)

    def test_raw_value_messages_go_to_the_blocks_as_bytes(self):
        self.stream(source=textwrap.dedent(SOURCE)
                    + 'serde_config:\n  serialization_method: RAW_VALUE\n')
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        source = captured.manifest['pipelines'][0]['stream']['source']['config']
        self.assertEqual(source['serde_config'], dict(serialization_method='RAW_VALUE'))

    def test_avro_without_a_registry_or_with_a_written_password_is_refused(self):
        self.stream(source=textwrap.dedent(SOURCE) + textwrap.dedent('''
            serde_config:
              serialization_method: AVRO
              schema_registry_password: hunter2
        '''))  # pragma: allowlist secret
        problems = self.problems()
        self.assertIn('AVRO messages need serde_config.schema_registry_url', problems)
        self.assertIn('serde_config.schema_registry_password is written in the file', problems)

    def test_a_sink_on_the_source_next_to_transformers_is_refused(self):
        source = self.stream()
        self.add('raw', 'data_exporter', PRINT, upstream=[source], language='yaml')
        self.assertIn("writes the source's messages next to transformers", self.problems())

    def test_cloud_secrets_in_connector_settings_are_fetched_by_the_service(self):
        self.stream(sink=textwrap.dedent(SINK)
                    + "sasl_config:\n  password: \"{{ aws_secret_var('kafka/pw') }}\"\n")
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        sink = captured.manifest['pipelines'][0]['stream']['sinks'][0]
        self.assertEqual(sink['config']['sasl_config']['password'],
                         {'$parts': [{'env': 'AWS_SECRET_KAFKA_PW'}]})
        variable = next(v for v in captured.manifest['environment']
                        if v['name'] == 'AWS_SECRET_KAFKA_PW')
        self.assertEqual(variable['source'], 'aws:kafka/pw')
        self.assertTrue(variable['secret'])

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
