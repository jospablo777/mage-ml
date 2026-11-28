"""
Streaming pipelines on Spark (ADR-0047): the plan from the export's capture, what Spark
cannot run refused with the list, and the blocks run by the engine's worker code on Arrow
batches, as mapInArrow runs them. integration_tests/spark runs them on a Spark Connect server.
"""
import json
import os
import sys
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from mage_ai.pipeline_services import spark_stream
from mage_ai.pipeline_services.runtime import spark_runner
from mage_ai.pipeline_services.spark_stream import SparkStreamError
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.pipeline_services.test_export_streams import (
    CLEAN,
    PRINT,
    SINK,
    StreamExportTestCase,
)

ENVIRON = dict(KAFKA_BOOTSTRAP='kafka:9092', KAFKA_USER='ana', KAFKA_PASSWORD='p"w')
SOURCE_WITH_TIME = '''
    connector_type: kafka
    bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
    topic: payments
    consumer_group: payment-totals
    event_time:
      field: paid_at
      format: rfc3339
      lateness_seconds: 30
      on_late: pass
'''
WINDOW = '''
    window:
      key: card_id
      type: session
      gap_seconds: 300
      aggregate:
        payments: count
        total: {sum: amount}
        shops: {distinct: shop_id}
'''


class SparkPlanTest(StreamExportTestCase):
    def test_a_kafka_stream_with_python_blocks_becomes_spark_options_and_a_chain(self):
        self.stream()

        stream = spark_stream.plan(self.repo_path, self.pipeline.uuid)
        source = spark_runner.source_options(stream, ENVIRON)

        self.assertEqual(source['kafka.bootstrap.servers'], 'kafka:9092')
        self.assertEqual(source['subscribe'], 'orders')
        self.assertEqual(source['startingOffsets'], 'earliest')
        self.assertEqual(source['maxOffsetsPerTrigger'], '250')
        self.assertEqual(source['kafka.security.protocol'], 'SASL_SSL')
        self.assertEqual(source['kafka.sasl.mechanism'], 'PLAIN')
        self.assertEqual(
            source['kafka.sasl.jaas.config'],
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="ana" password="p\\"w";',
        )
        self.assertEqual(spark_runner.sink_options(stream['sinks'][0], ENVIRON),
                         {'kafka.bootstrap.servers': 'kafka:9092', 'topic': 'orders_clean'})
        self.assertEqual(spark_runner.trigger_seconds(stream), 0.2)
        self.assertEqual([b['uuid'] for b in stream['chain']], [f'{self.pipeline.uuid}_clean'])
        self.assertIn('@transformer', stream['chain'][0]['code'])
        self.assertEqual(stream['sinks'][0]['after'], f'{self.pipeline.uuid}_clean')
        self.assertIsNone(stream['dead_letter'])
        self.assertIn('checkpoint', spark_stream.notes(stream)[0])
        # The plan keeps the references; the password is filled in when it starts.
        self.assertNotIn('p\\"w', json.dumps(stream))

    def test_a_window_block_is_planned_with_its_event_time(self):
        source = self.add('source', 'data_loader', SOURCE_WITH_TIME, language='yaml')
        clean = self.add('clean', 'transformer', CLEAN, upstream=[source])
        window = self.add('window', 'transformer', WINDOW, upstream=[clean], language='yaml')
        self.add('totals', 'data_exporter', SINK, upstream=[window], language='yaml')

        stream = spark_stream.plan(self.repo_path, self.pipeline.uuid)

        self.assertEqual([b['uuid'] for b in stream['chain']], [clean.uuid])
        self.assertEqual(stream['window'], dict(
            uuid=window.uuid, upstream=[clean.uuid], spec=dict(
                key='card_id', kind=dict(type='session', gap_ms=300000),
                aggregates=[['payments', 'count'], ['total', {'sum': 'amount'}],
                            ['shops', {'distinct': 'shop_id'}]],
            )))
        self.assertEqual(stream['time'], dict(field='paid_at', format='rfc3339',
                                              on_late='pass', lateness_ms=30000))
        self.assertEqual(stream['sinks'][0]['after'], window.uuid)
        notes = '\n'.join(spark_stream.notes(stream))
        self.assertIn('one watermark for the query', notes)
        self.assertIn('Distinct counts compare values by their text', notes)

    def test_the_command_checks_a_pipeline_without_starting_it(self):
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        self.stream()
        args = ['spark-stream', self.repo_path, self.pipeline.uuid, '--remote', 'sc://nowhere',
                '--checkpoint', '/checkpoints/x', '--check']
        variables = ['--env', 'KAFKA_BOOTSTRAP=kafka:9092', '--env', 'KAFKA_USER=ana']
        ready = CliRunner().invoke(app, args + variables + ['--env', 'KAFKA_PASSWORD=pw'])
        self.assertEqual(ready.exit_code, 0, ready.output)
        self.assertIn('can run on Spark', ready.output)
        self.assertIn('checkpoint', ready.output)
        unset = CliRunner().invoke(app, args + variables)
        self.assertEqual(unset.exit_code, 2)
        self.assertIn('KAFKA_PASSWORD', unset.output)

    def test_an_unset_variable_and_an_unsupported_sink_are_named(self):
        self.stream()
        stream = spark_stream.plan(self.repo_path, self.pipeline.uuid)
        with self.assertRaisesRegex(SparkStreamError, 'KAFKA_PASSWORD, which is not set'):
            spark_stream.check(stream, dict(KAFKA_BOOTSTRAP='kafka:9092', KAFKA_USER='ana'))
        self.add('print', 'data_exporter', PRINT, language='yaml', upstream=[
            self.pipeline.get_block(f'{self.pipeline.uuid}_clean')])
        with self.assertRaises(SparkStreamError) as caught:
            spark_stream.plan(self.repo_path, self.pipeline.uuid)
        self.assertIn('cannot run on Spark as it runs on the engine', str(caught.exception))
        self.assertIn('is stdout; Spark writes Kafka', str(caught.exception))


    def test_the_export_runs_without_mageml_and_holds_no_secret(self):
        self.stream()
        folder = Path(tempfile.mkdtemp()) / 'spark'

        spark_stream.export(self.repo_path, self.pipeline.uuid, str(folder))

        self.assertEqual(sorted(p.name for p in folder.iterdir()), [
            'Dockerfile', 'README.md', 'plan.json', 'spark_runner.py', 'worker.py',
        ])
        plan = json.loads((folder / 'plan.json').read_text())
        self.assertEqual(plan['sinks'][0]['after'], f'{self.pipeline.uuid}_clean')
        self.assertIn('{"env": "KAFKA_PASSWORD"}', json.dumps(plan))
        dockerfile = (folder / 'Dockerfile').read_text()
        # The client's Python, which Spark's workers must run too.
        self.assertIn(f'FROM python:{sys.version_info[0]}.{sys.version_info[1]}-slim', dockerfile)
        self.assertRegex(dockerfile, r'pyspark-client==4\.\d+\.\d+ pyarrow==\d+')
        readme = (folder / 'README.md').read_text()
        for name in ('KAFKA_BOOTSTRAP', 'KAFKA_PASSWORD', 'KAFKA_USER', 'MAGE_SPARK_REMOTE'):
            self.assertIn(f'-e {name}', readme)
        # The runner imports nothing of MageML.
        runner = (folder / 'spark_runner.py').read_text()
        self.assertNotIn('mage_ai', runner.split('"""', 2)[2])


class RunnerTest(TestCase):
    def test_the_runner_needs_its_server_and_checkpoint(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(spark_runner.main(), 2)
        with self.assertRaisesRegex(spark_runner.SparkRunError, 'checkpoint location'):
            spark_runner.start(dict(pipeline_uuid='p'), 'sc://x', '')


class DifferencesTest(TestCase):
    def test_each_engine_only_feature_is_named(self):
        pipeline = dict(
            stream=dict(
                source=dict(kind='kafka', config=dict(topic='a', librdkafka={'x': 1})),
                sinks=[dict(name='out', kind='postgres', config={})],
                errors=dict(dead_letter=dict(name='dead', kind='http', config={})),
                delivery='exactly_once',
                time=dict(field='at'),
            ),
            blocks=[
                dict(uuid='totals', language='window', type='transformer'),
                dict(uuid='score', language='python', type='transformer',
                     configuration=dict(state_key='card')),
                dict(uuid='fine', language='python', type='transformer'),
            ],
        )
        found = '\n'.join(spark_stream.differences(pipeline))
        for expected in [
            "settings librdkafka have no Spark equivalent",
            'Sink out is postgres',
            'Sink dead is http',
            'Delivery exactly_once',
            'Late messages go to the dead-letter sink (on_late: dead_letter)',
            'A dead-letter sink next to window totals',
            'Block score uses state_key',
        ]:
            self.assertIn(expected, found)
        self.assertNotIn('fine', found)
        self.assertEqual(spark_stream.differences(dict(
            stream=dict(source=dict(kind='kafka', config=dict(topic='a')),
                        sinks=[dict(name='out', kind='kafka', config=dict(topic='b'))]),
            blocks=[dict(uuid='fine', language='python', type='transformer')],
        )), [])
        # Event time without a window.
        self.assertIn('Event time and late messages without a window', '\n'.join(
            spark_stream.differences(dict(
                stream=dict(source=dict(kind='kafka', config=dict(topic='a')),
                            sinks=[dict(name='out', kind='kafka', config=dict(topic='b'))],
                            time=dict(field='at', on_late='pass')),
                blocks=[],
            ))))

    def test_a_window_runs_last_after_python_blocks_with_a_field_time(self):
        def window(uuid='totals', upstream=(), key='card_id', field='amount'):
            return dict(uuid=uuid, language='window', type='transformer',
                        upstream=list(upstream), window=dict(
                            key=key, kind=dict(type='tumbling', size_ms=60000),
                            aggregates=[['payments', 'count'], ['total', {'sum': field}]]))

        def found(blocks, sinks=None, time=None):
            return '\n'.join(spark_stream.differences(dict(
                stream=dict(source=dict(kind='kafka', config=dict(topic='a')),
                            sinks=sinks or [dict(name='out', kind='kafka',
                                                 config=dict(topic='b'))],
                            time=time),
                blocks=blocks,
            )))

        clean = dict(uuid='clean', language='python', type='transformer')
        passed = dict(field='at', format='rfc3339', on_late='pass', lateness_ms=5000)
        self.assertEqual(found([clean, window(upstream=['clean'])], time=passed), '')
        # The source's timestamp, without blocks before the window.
        self.assertEqual(found([window()]), '')
        for blocks, sinks, time, expected in [
            ([window(), window('more')], None, passed, 'one window block per stream'),
            ([window(), dict(clean, upstream=['totals'])], None, passed,
             'Block clean follows window totals'),
            ([clean, window(upstream=['clean'])],
             [dict(name='raw', kind='kafka', config=dict(topic='b'), after='clean')], passed,
             'Sink raw follows clean, not window totals'),
            ([clean, window(upstream=['clean'])], None, None,
             'Spark reads event time from a field'),
            ([window(key='card id')], None, passed, 'Field card id: Spark reads fields'),
            ([window(field='a[0]')], None, passed, 'Field a[0]: Spark reads fields'),
        ]:
            self.assertIn(expected, found(blocks, sinks, time))


DOUBLE = '''
from typing import Dict, List


@transformer
def double(messages: List[Dict], *args, **kwargs):
    for m in messages:
        if isinstance(m, dict) and m.get('id') in kwargs.get('reject', []):
            raise ValueError(f"message {m['id']} is not accepted")
    return [dict(m, doubled=m['amount'] * 2) if isinstance(m, dict) else {'text': m}
            for m in messages]
'''
KEEP = '''
import polars as pl


@transformer
def keep(frame: pl.DataFrame, *args, **kwargs):
    return frame.filter(pl.col('doubled') > kwargs['least'])
'''


def batch(values):
    return pa.RecordBatch.from_pydict({'value': [v.encode() for v in values]},
                                      schema=pa.schema([('value', pa.binary())]))


WORKER = spark_stream.RUNTIME.joinpath('worker.py').read_text()


class BlocksFunctionTest(TestCase):
    def stream(self, dead_letter=None, **variables):
        return dict(
            pipeline_uuid='orders',
            chain=[
                dict(uuid='double', code=textwrap.dedent(DOUBLE), upstream=[], configuration={}),
                dict(uuid='keep', code=textwrap.dedent(KEEP), upstream=['double'],
                     configuration={}),
            ],
            sinks=[dict(name='out', config={}, after='keep')],
            dead_letter=dead_letter, variables=dict(least=5, **variables),
        )

    def test_the_blocks_run_on_each_arrow_batch_as_the_engine_runs_them(self):
        run = spark_runner.blocks_function(self.stream(), WORKER)
        out = list(run(iter([
            batch(['{"id": 1, "amount": 2}', '{"id": 2, "amount": 4}']),
            batch([]),
            batch(['{"id": 3, "amount": 3.5}']),
        ])))
        rows = [v for b in out for v in b.column('value').to_pylist()]
        # A DataFrame block's output goes out as one JSON object per row.
        self.assertEqual([json.loads(v) for v in rows], [
            {'id': 2, 'amount': 4, 'doubled': 8}, {'id': 3, 'amount': 3.5, 'doubled': 7.0},
        ])
        self.assertEqual({t for b in out for t in b.column('target').to_pylist()}, {'keep'})

    def test_text_that_is_not_json_reaches_the_blocks_as_text(self):
        stream = self.stream()
        stream['chain'] = stream['chain'][:1]
        stream['sinks'] = [dict(name='out', config={}, after='double')]
        run = spark_runner.blocks_function(stream, WORKER)
        [out] = list(run(iter([batch(['plain words'])])))
        self.assertEqual(out.column('value').to_pylist(), [b'{"text": "plain words"}'])

    def test_a_failing_block_fails_the_batch_with_its_name_and_reason(self):
        run = spark_runner.blocks_function(self.stream(reject=[2]), WORKER)
        with self.assertRaisesRegex(RuntimeError,
                                    r'Block double failed \(run\): .*message 2 is not accepted'):
            list(run(iter([batch(['{"id": 2, "amount": 4}'])])))

    def test_a_message_a_block_fails_on_goes_to_dead_letters_as_it_came(self):
        run = spark_runner.blocks_function(
            self.stream(dead_letter=dict(name='dead', config={}), reject=[2]), WORKER,
        )
        incoming = pa.RecordBatch.from_pydict({
            'key': [b'k1', b'k2', b'k3'],
            'value': [b'{"id": 1, "amount": 4}', b'{"id": 2, "amount": 4}',
                      b'{"id": 3, "amount": 4}'],
            'headers': [None, [dict(key='trace', value=b't2')], None],
        })
        [out] = list(run(iter([incoming])))
        rows = out.to_pylist()
        self.assertEqual([r['target'] for r in rows], ['keep', 'keep', '__dead_letter__'])
        dead = rows[-1]
        self.assertEqual((dead['key'], dead['value']), (b'k2', b'{"id": 2, "amount": 4}'))
        self.assertEqual([h['key'] for h in dead['headers']],
                         ['trace', 'mage-stream', 'mage-error'])
        self.assertIn(b'double: ValueError: message 2 is not accepted',
                      dead['headers'][-1]['value'])
        # Every message failing is a code problem: the batch fails, nothing is a dead letter.
        all_bad = spark_runner.blocks_function(
            self.stream(dead_letter=dict(name='dead', config={}), reject=[1, 2]), WORKER,
        )
        with self.assertRaisesRegex(RuntimeError, 'Block double failed'):
            list(all_bad(iter([batch(['{"id": 1, "amount": 4}', '{"id": 2, "amount": 4}'])])))

    def test_progress_reads_as_one_line(self):
        self.assertEqual(
            spark_runner.progress_line(dict(
                batchId=4, numInputRows=120, inputRowsPerSecond=60.4,
                processedRowsPerSecond=300.0, sink=dict(numOutputRows=110),
            )),
            'batch 4: 120 rows in, 60/s arriving, 300/s processed, 110 rows out',
        )
        self.assertIsNone(spark_runner.progress_line(None))
