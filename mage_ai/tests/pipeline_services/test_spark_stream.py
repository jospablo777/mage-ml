"""
Streaming pipelines on Spark (ADR-0047): the plan from the export's capture, what Spark
cannot run refused with the list, and the blocks run by the engine's worker code on Arrow
batches, as mapInArrow runs them. integration_tests/spark runs them on a Spark Connect server.
"""
import textwrap

import pyarrow as pa

from mage_ai.pipeline_services import spark_stream
from mage_ai.pipeline_services.spark_stream import SparkStream, SparkStreamError
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.pipeline_services.test_export_streams import (
    PRINT,
    StreamExportTestCase,
)

ENVIRON = dict(KAFKA_BOOTSTRAP='kafka:9092', KAFKA_USER='ana', KAFKA_PASSWORD='p"w')


class SparkPlanTest(StreamExportTestCase):
    def test_a_kafka_stream_with_python_blocks_becomes_spark_options_and_a_chain(self):
        self.stream()

        stream = spark_stream.plan(self.repo_path, self.pipeline.uuid, environ=ENVIRON)

        self.assertEqual(stream.source['kafka.bootstrap.servers'], 'kafka:9092')
        self.assertEqual(stream.source['subscribe'], 'orders')
        self.assertEqual(stream.source['startingOffsets'], 'earliest')
        self.assertEqual(stream.source['maxOffsetsPerTrigger'], '250')
        self.assertEqual(stream.source['kafka.security.protocol'], 'SASL_SSL')
        self.assertEqual(stream.source['kafka.sasl.mechanism'], 'PLAIN')
        self.assertEqual(
            stream.source['kafka.sasl.jaas.config'],
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="ana" password="p\\"w";',
        )
        self.assertEqual(stream.sink, {'kafka.bootstrap.servers': 'kafka:9092',
                                       'topic': 'orders_clean'})
        self.assertEqual(stream.trigger_seconds, 0.2)
        self.assertEqual([b['uuid'] for b in stream.chain], [f'{self.pipeline.uuid}_clean'])
        self.assertIn('@transformer', stream.chain[0]['code'])
        self.assertEqual(stream.output, f'{self.pipeline.uuid}_clean')
        self.assertIn('checkpoint', stream.notes[0])

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
        with self.assertRaisesRegex(SparkStreamError, 'KAFKA_PASSWORD, which is not set'):
            spark_stream.plan(self.repo_path, self.pipeline.uuid,
                              environ=dict(KAFKA_BOOTSTRAP='kafka:9092', KAFKA_USER='ana'))
        self.add('print', 'data_exporter', PRINT, language='yaml', upstream=[
            self.pipeline.get_block(f'{self.pipeline.uuid}_clean')])
        with self.assertRaises(SparkStreamError) as caught:
            spark_stream.plan(self.repo_path, self.pipeline.uuid, environ=ENVIRON)
        self.assertIn('cannot run on Spark as it runs on the engine', str(caught.exception))
        self.assertIn('The stream has 2 sinks', str(caught.exception))
        self.assertIn('is stdout; Spark writes Kafka', str(caught.exception))


class DifferencesTest(TestCase):
    def test_each_engine_only_feature_is_named(self):
        pipeline = dict(
            stream=dict(
                source=dict(kind='kafka', config=dict(topic='a', librdkafka={'x': 1})),
                sinks=[dict(name='out', kind='postgres', config={})],
                errors=dict(dead_letter=dict(name='dead')),
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
            'Dead letters',
            'Delivery exactly_once',
            'Event time',
            'Block totals is a window transformer',
            'Block score uses state_key',
        ]:
            self.assertIn(expected, found)
        self.assertNotIn('fine', found)
        self.assertEqual(spark_stream.differences(dict(
            stream=dict(source=dict(kind='kafka', config=dict(topic='a')),
                        sinks=[dict(name='out', kind='kafka', config=dict(topic='b'))]),
            blocks=[dict(uuid='fine', language='python', type='transformer')],
        )), [])


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


class BlocksFunctionTest(TestCase):
    def stream(self, **variables):
        return SparkStream(
            pipeline_uuid='orders', source={}, sink={},
            chain=[
                dict(uuid='double', code=textwrap.dedent(DOUBLE), upstream=[], configuration={}),
                dict(uuid='keep', code=textwrap.dedent(KEEP), upstream=['double'],
                     configuration={}),
            ],
            output='keep', variables=dict(least=5, **variables),
        )

    def test_the_blocks_run_on_each_arrow_batch_as_the_engine_runs_them(self):
        run = spark_stream.blocks_function(self.stream())
        out = list(run(iter([
            batch(['{"id": 1, "amount": 2}', '{"id": 2, "amount": 4}']),
            batch([]),
            batch(['{"id": 3, "amount": 3.5}']),
        ])))
        rows = [v for b in out for v in b.column('value').to_pylist()]
        # A DataFrame block's output goes out as one JSON object per row.
        import json
        self.assertEqual([json.loads(v) for v in rows], [
            {'id': 2, 'amount': 4, 'doubled': 8}, {'id': 3, 'amount': 3.5, 'doubled': 7.0},
        ])
        self.assertEqual(out[0].schema, pa.schema([('value', pa.binary())]))

    def test_text_that_is_not_json_reaches_the_blocks_as_text(self):
        stream = self.stream()
        stream.chain = stream.chain[:1]
        stream.output = 'double'
        run = spark_stream.blocks_function(stream)
        [out] = list(run(iter([batch(['plain words'])])))
        self.assertEqual(out.column('value').to_pylist(), [b'{"text": "plain words"}'])

    def test_a_failing_block_fails_the_batch_with_its_name_and_reason(self):
        run = spark_stream.blocks_function(self.stream(reject=[2]))
        with self.assertRaisesRegex(RuntimeError,
                                    r'Block double failed \(run\): .*message 2 is not accepted'):
            list(run(iter([batch(['{"id": 2, "amount": 4}'])])))

    def test_progress_reads_as_one_line(self):
        self.assertEqual(
            spark_stream.progress_line(dict(
                batchId=4, numInputRows=120, inputRowsPerSecond=60.4,
                processedRowsPerSecond=300.0, sink=dict(numOutputRows=110),
            )),
            'batch 4: 120 rows in, 60/s arriving, 300/s processed, 110 rows out',
        )
        self.assertIsNone(spark_stream.progress_line(None))
