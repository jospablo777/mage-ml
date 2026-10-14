"""
Stream samples for the editor: files of JSON lines next to the pipeline, and blocks run on
them with the service engine's Python worker.
"""
import textwrap
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.constants import PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.streaming import sample_runner, samples
from mage_ai.tests.base_test import DBTestCase

SOURCE = '''
connector_type: kafka
bootstrap_server: localhost:9092
topic: orders
consumer_group: orders
'''


class StreamSampleTestCase(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(
            f'{self._testMethodName}_stream', pipeline_type=PipelineType.STREAMING,
            repo_path=self.repo_path,
        )
        self.addCleanup(self.pipeline.delete)
        self.source = self.add('source', 'data_loader', SOURCE, language='yaml')

    def add(self, name, kind, code, upstream=(), language='python'):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language=language,
            pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block


class SampleFilesTest(StreamSampleTestCase):
    def test_samples_are_json_lines_next_to_the_pipeline(self):
        messages = [{'id': 1, 'big': 123456789012345678901234567890}, 'text', None]

        summary = samples.write_sample(self.repo_path, self.pipeline.uuid, 'orders', messages)

        path = Path(self.repo_path) / 'pipelines' / self.pipeline.uuid / 'samples' / 'orders.jsonl'
        self.assertEqual(path.read_text().splitlines()[1], '"text"')
        self.assertEqual(summary['name'], 'orders')
        self.assertEqual(summary['message_count'], 3)
        self.assertEqual(samples.read_sample(self.repo_path, self.pipeline.uuid, 'orders'),
                         messages)
        self.assertEqual([s['name'] for s in samples.list_samples(
            self.repo_path, self.pipeline.uuid)], ['orders'])
        samples.delete_sample(self.repo_path, self.pipeline.uuid, 'orders')
        self.assertEqual(samples.list_samples(self.repo_path, self.pipeline.uuid), [])

    def test_names_and_values_are_checked(self):
        for name in ('../escape', '', '.hidden', 'a/b', 'x.jsonl'):
            with self.assertRaises(samples.SampleError):
                samples.write_sample(self.repo_path, self.pipeline.uuid, name, [])
        with self.assertRaisesRegex(samples.SampleError, 'NaN or infinity'):
            samples.write_sample(self.repo_path, self.pipeline.uuid, 'n', [{'x': float('nan')}])
        with self.assertRaisesRegex(samples.SampleError, 'neither JSON nor UTF-8'):
            samples.message_value(b'\xff\xfe')
        self.assertEqual(samples.message_value(b'{"a": 1}'), {'a': 1})
        self.assertEqual(samples.message_value(b'plain'), 'plain')

    def test_the_newest_messages_are_shared_by_what_each_partition_holds(self):
        self.assertEqual(samples._shares(5, {'a': 40, 'b': 0}), {'a': 5, 'b': 0})
        self.assertEqual(samples._shares(6, {'a': 40, 'b': 40, 'c': 1}),
                         {'c': 1, 'a': 3, 'b': 2})
        self.assertEqual(samples._shares(100, {'a': 3, 'b': 4}), {'a': 3, 'b': 4})

    def test_captured_samples_can_stay_out_of_git(self):
        samples.ignore_in_git(self.repo_path, self.pipeline.uuid)
        ignore = samples.samples_dir(self.repo_path, self.pipeline.uuid) / '.gitignore'
        self.assertIn('*.jsonl', ignore.read_text())

    def test_capturing_from_another_connector_explains_what_to_do(self):
        Path(self.source.file_path).write_text('connector_type: rabbitmq\n')
        with self.assertRaisesRegex(samples.SampleError, 'write or paste a sample'):
            samples.capture_sample(self.repo_path, self.pipeline.uuid, 'r')


class WebhookCheckTest(StreamSampleTestCase):
    def test_checking_a_webhook_source_explains_how_it_receives_messages(self):
        import contextlib
        import io

        from mage_ai.data_preparation.models.block.streaming import check_connection

        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            check_connection('data_loader', 'connector_type: webhook\n')

        self.assertIn('/v1/streams/<pipeline>/messages', printed.getvalue())


class RunOnSampleTest(StreamSampleTestCase):
    def setUp(self):
        super().setUp()
        self.clean = self.add('clean', 'transformer', '''
            @transformer
            def clean(messages, *args, **kwargs):
                print(f'cleaning {len(messages)}')
                return [m for m in messages if m.get('amount') is not None]
        ''', upstream=[self.source])
        self.score = self.add('score', 'transformer', '''
            @transformer
            def score(messages, *args, **kwargs):
                return [dict(m, score=100 / m['amount']) for m in messages]
        ''', upstream=[self.clean])

    def tearDown(self):
        sample_runner.stop_workers()
        super().tearDown()

    def test_a_block_runs_with_the_blocks_before_it_on_the_messages(self):
        messages = [{'amount': 4}, {'amount': None}, {'amount': 5}]

        result = sample_runner.run_on_sample(
            self.repo_path, self.pipeline.uuid, self.score.uuid, messages,
        )

        self.assertNotIn('error', result)
        self.assertEqual(result['outputs'][self.clean.uuid], [{'amount': 4}, {'amount': 5}])
        self.assertEqual(result['outputs'][self.score.uuid],
                         [{'amount': 4, 'score': 25.0}, {'amount': 5, 'score': 20.0}])
        self.assertEqual([b['block_uuid'] for b in result['blocks']],
                         [self.clean.uuid, self.score.uuid])
        self.assertIn(dict(block_uuid=self.clean.uuid, line='cleaning 3'), result['logs'])

    def test_a_failure_points_at_the_first_message_that_fails(self):
        messages = [{'amount': 4}, {'amount': 0}, {'amount': 5}]

        result = sample_runner.run_on_sample(
            self.repo_path, self.pipeline.uuid, self.score.uuid, messages,
        )

        self.assertEqual(result['error']['block_uuid'], self.score.uuid)
        self.assertIn('ZeroDivisionError', result['error']['message'])
        self.assertEqual(result['failing_message'], dict(index=1, message={'amount': 0}))

    def test_an_edited_block_runs_its_new_code(self):
        messages = [{'amount': 4}]
        sample_runner.run_on_sample(self.repo_path, self.pipeline.uuid, self.score.uuid,
                                    messages)
        Path(self.score.file_path).write_text(textwrap.dedent('''
            @transformer
            def score(messages, *args, **kwargs):
                return [dict(m, score=-1) for m in messages]
        '''))

        result = sample_runner.run_on_sample(
            self.repo_path, self.pipeline.uuid, self.score.uuid, messages,
        )

        self.assertEqual(result['outputs'][self.score.uuid], [{'amount': 4, 'score': -1}])


class SampleApiTest(StreamSampleTestCase):
    """The API the editor uses: /api/pipelines/<uuid>/stream_samples and stream_sample_runs."""

    def setUp(self):
        super().setUp()
        from mage_ai.tests.factory import create_user

        self.owner = create_user(_owner=True)
        self.add('double', 'transformer', '''
            @transformer
            def double(messages, *args, **kwargs):
                return [dict(m, twice=m['n'] * 2) for m in messages]
        ''', upstream=[self.source])

    def tearDown(self):
        sample_runner.stop_workers()
        super().tearDown()

    def call(self, action, resource='stream_samples', **kwargs):
        import asyncio

        from mage_ai.api.operations.base import BaseOperation

        return asyncio.run(BaseOperation(
            action=action, resource=resource, resource_parent='pipelines',
            resource_parent_id=self.pipeline.uuid, user=self.owner, **kwargs,
        ).execute())

    def test_save_list_read_edit_run_and_delete_a_sample(self):
        from mage_ai.api.operations.constants import OperationType

        created = self.call(OperationType.CREATE, payload=dict(
            stream_sample=dict(name='numbers', messages=[{'n': 1}, {'n': 2}]),
        ))
        self.assertIn('stream_sample', created, created)
        self.assertEqual(created['stream_sample']['message_count'], 2, created)
        listed = self.call(OperationType.LIST)
        self.assertEqual([s['name'] for s in listed['stream_samples']], ['numbers'])
        self.call(OperationType.UPDATE, pk='numbers', payload=dict(
            stream_sample=dict(messages=[{'n': 3}]),
        ))
        detail = self.call(OperationType.DETAIL, pk='numbers')
        self.assertEqual(detail['stream_sample']['messages'], [{'n': 3}])

        run = self.call(OperationType.CREATE, resource='stream_sample_runs', payload=dict(
            stream_sample_run=dict(block_uuid=f'{self.pipeline.uuid}_double', sample='numbers'),
        ))['stream_sample_run']
        self.assertEqual(run['outputs'][f'{self.pipeline.uuid}_double'], [{'n': 3, 'twice': 6}])
        self.assertEqual(run['messages'], [{'n': 3}])

        self.call(OperationType.DELETE, pk='numbers')
        self.assertEqual(self.call(OperationType.LIST)['stream_samples'], [])

    def test_errors_say_what_is_wrong(self):
        from mage_ai.api.operations.constants import OperationType

        missing = self.call(OperationType.DETAIL, pk='nothing')
        self.assertEqual(missing['error']['code'], 404, missing)
        bad = self.call(OperationType.CREATE, payload=dict(
            stream_sample=dict(name='../x', messages=[]),
        ))
        self.assertIn('not a sample name', bad['error']['message'])


class RustSampleRunTest(StreamSampleTestCase):
    """
    Rust stream blocks run on samples in their programs, over the protocol the service
    engine uses. Builds real binaries (the dependencies once, into the shared target).
    """

    PRICE = '''
        use mage::prelude::*;

        fn transform(messages: Vec<Value>, vars: Vars) -> Result<Vec<Value>> {
            let rate: f64 = vars.get_or("rate", 1.0)?;
            println!("pricing {}", messages.len());
            messages
                .into_iter()
                .map(|mut m| {
                    let amount = m["amount"].as_f64().context("no amount")?;
                    m["total"] = json!(amount * rate);
                    Ok(m)
                })
                .collect()
        }
    '''
    LARGE = '''
        use mage::prelude::*;

        fn transform(orders: DataFrame) -> Result<DataFrame> {
            Ok(orders.lazy().filter(col("total").gt(lit(5.0))).collect()?)
        }
    '''

    def test_each_rust_block_runs_on_the_messages_of_the_one_before(self):
        price = self.add('price', 'transformer', self.PRICE, [self.source], language='rust')
        large = self.add('large', 'transformer', self.LARGE, [price], language='rust')

        result = sample_runner.run_on_sample(
            self.repo_path, self.pipeline.uuid, large.uuid,
            [{'amount': 1}, {'amount': 4}], variables=dict(rate=2.0),
        )

        self.assertNotIn('error', result, result)
        self.assertEqual(result['outputs'][price.uuid],
                         [{'amount': 1, 'total': 2.0}, {'amount': 4, 'total': 8.0}])
        self.assertEqual(result['outputs'][large.uuid], [{'amount': 4, 'total': 8.0}])
        self.assertIn(dict(block_uuid=price.uuid, line='pricing 2'), result['logs'])

    def test_a_failing_rust_block_names_its_error_and_first_failing_message(self):
        price = self.add('price', 'transformer', self.PRICE, [self.source], language='rust')

        result = sample_runner.run_on_sample(
            self.repo_path, self.pipeline.uuid, price.uuid,
            [{'amount': 1}, {'id': 7}, {'amount': 3}],
        )

        self.assertEqual(result['error']['block_uuid'], price.uuid)
        self.assertIn('no amount', result['error']['message'])
        self.assertEqual(result['failing_message'], dict(index=1, message={'id': 7}))

    def test_python_and_rust_blocks_in_one_chain_run_in_turn(self):
        python = self.add('py', 'transformer', '''
            @transformer
            def f(messages, *args, **kwargs):
                print('python first')
                return [dict(m, amount=m['amount'] + 1) for m in messages]
        ''', [self.source])
        price = self.add('price', 'transformer', self.PRICE, [python], language='rust')
        after = self.add('after', 'transformer', '''
            @transformer
            def f(messages, *args, **kwargs):
                return [dict(m, last=True) for m in messages]
        ''', [price])

        result = sample_runner.run_on_sample(
            self.repo_path, self.pipeline.uuid, after.uuid, [{'amount': 1}],
            variables=dict(rate=10.0),
        )

        self.assertNotIn('error', result, result)
        self.assertEqual(result['outputs'][python.uuid], [{'amount': 2}])
        self.assertEqual(result['outputs'][price.uuid], [{'amount': 2, 'total': 20.0}])
        self.assertEqual(result['outputs'][after.uuid],
                         [{'amount': 2, 'total': 20.0, 'last': True}])
        self.assertIn(dict(block_uuid=python.uuid, line='python first'), result['logs'])
