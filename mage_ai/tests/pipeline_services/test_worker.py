import io
import json
import os
import tempfile
import textwrap
from datetime import datetime

import numpy as np
import pandas as pd
import polars as pl

from mage_ai.pipeline_services.runtime import worker
from mage_ai.tests.base_test import TestCase


class WorkerTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.directory = tempfile.mkdtemp()

    def block(self, code: str, block_type: str = 'transformer', name: str = 'block') -> dict:
        path = os.path.join(self.directory, f'{name}.py')
        with open(path, 'w') as file:
            file.write(textwrap.dedent(code))
        return dict(uuid=name, type=block_type, file=path, configuration={'a': 1})

    def run_block(self, code: str, inputs=(), kwargs=None, block_type='transformer', **extra):
        request = dict(
            block=self.block(code, block_type),
            inputs=list(inputs),
            kwargs=kwargs or {},
            output_dir=os.path.join(self.directory, 'out'),
            **extra,
        )
        return worker.run_block(request)

    def write(self, value, name='in') -> dict:
        directory = os.path.join(self.directory, name)
        os.makedirs(directory, exist_ok=True)
        return worker.write_output(value, directory, 0)


class OutputsTest(WorkerTestCase):
    def test_a_returned_list_or_tuple_is_several_outputs_and_none_is_none(self):
        result = self.run_block('''
            @transformer
            def f():
                return [1, {'a': 2}]
        ''')
        self.assertEqual([o['kind'] for o in result['outputs']], ['json', 'json'])
        self.assertEqual(len(self.run_block('''
            @transformer
            def f():
                return (1, 2, 3)
        ''')['outputs']), 3)
        self.assertEqual(self.run_block('''
            @transformer
            def f():
                return None
        ''')['outputs'], [])

    def test_tables_keep_their_types_through_files(self):
        frame = pd.DataFrame({
            'n': pd.Series([1, None], dtype='Int64'),
            'when': pd.to_datetime(['2026-01-01', '2026-01-02']).tz_localize('UTC'),
            'kind': pd.Categorical(['a', 'b']),
            'text': pd.Series(['x', None], dtype='str'),
        }, index=pd.Index(['r1', 'r2'], name='row'))
        record = self.write(frame)
        self.assertEqual(record['origin'], 'pandas')
        back = worker.read_input(record)
        pd.testing.assert_frame_equal(back, frame)

        polars_frame = pl.DataFrame({'a': [1, None], 'b': ['x', 'y']})
        record = self.write(polars_frame, 'pl')
        self.assertEqual(record['rows'], 2)
        self.assertTrue(worker.read_input(record).equals(polars_frame))

        lazy = self.write(polars_frame.lazy().filter(pl.col('b') == 'y'), 'lazy')
        self.assertEqual(lazy['rows'], 1)
        self.assertEqual(worker.read_input(lazy).to_dicts(), [{'a': None, 'b': 'y'}])

        series = pd.Series([1.5, 2.5], name='x')
        pd.testing.assert_series_equal(worker.read_input(self.write(series, 's')), series)

    def test_arrow_backed_pandas_columns_come_back(self):
        import pyarrow as pa

        frame = pd.DataFrame({
            'id': [1, 2],
            'tags': pd.Series([[1, 2], [3]], dtype=pd.ArrowDtype(pa.list_(pa.int64()))),
            'score': pd.Series([1.5, None], dtype=pd.ArrowDtype(pa.float64())),
        })
        back = worker.read_input(self.write(frame))
        pd.testing.assert_frame_equal(back, frame)

    def test_other_values_choose_a_format_that_keeps_them(self):
        array = np.arange(6).reshape(2, 3)
        record = self.write(array, 'np')
        self.assertEqual(record['kind'], 'numpy')
        np.testing.assert_array_equal(worker.read_input(record), array)
        # NaN, tuples and non-string keys would change in JSON.
        for value in (float('nan'), {1: 'a'}, (1, 2), datetime(2026, 1, 1)):
            record = self.write(value, f'v{id(value)}')
            self.assertEqual(record['kind'], 'pickle', value)
        back = worker.read_input(self.write({1: 'a'}, 'k'))
        self.assertEqual(back, {1: 'a'})

    def test_an_upstream_with_several_outputs_arrives_as_a_list(self):
        first = self.write(1, 'a')
        second = self.write('two', 'b')
        result = self.run_block('''
            @transformer
            def f(values):
                return {'got': values}
        ''', inputs=[dict(kind='list', items=[first, second])])
        with open(result['outputs'][0]['path']) as file:
            self.assertEqual(json.load(file), {'got': [1, 'two']})


class AsyncBlockTest(WorkerTestCase):
    def test_an_async_block_function_runs_in_a_service(self):
        """An async def block returned its coroutine unrun (upstream #5164)."""
        result = self.run_block('''
            import asyncio
            @transformer
            async def f():
                await asyncio.sleep(0)
                return {'n': 42}
        ''')
        with open(result['outputs'][0]['path']) as file:
            self.assertEqual(json.load(file), {'n': 42})


class ArgumentsTest(WorkerTestCase):
    def test_kwargs_reach_only_functions_that_take_them(self):
        result = self.run_block('''
            @data_loader
            def load(*args, **kwargs):
                return {
                    'date': kwargs['execution_date'].isoformat(),
                    'rows': kwargs['rows'],
                    'configuration': kwargs['configuration'],
                    'block': kwargs['block_uuid'],
                }
        ''', kwargs={'execution_date': '2026-10-10T05:00:00.000Z', 'rows': 3},
            block_type='data_loader')
        with open(result['outputs'][0]['path']) as file:
            self.assertEqual(json.load(file), {
                'date': '2026-10-10T05:00:00', 'rows': 3, 'configuration': {'a': 1},
                'block': 'block',
            })
        result = self.run_block('''
            @data_loader
            def load():
                return 1
        ''', kwargs={'rows': 3}, block_type='data_loader')
        self.assertEqual(len(result['outputs']), 1)

    def test_upstream_outputs_arrive_in_order(self):
        a, b = self.write('a', 'x'), self.write('b', 'y')
        result = self.run_block('''
            @transformer
            def f(first, second):
                return first + second
        ''', inputs=[a, b])
        with open(result['outputs'][0]['path']) as file:
            self.assertEqual(json.load(file), 'ab')


class FailuresTest(WorkerTestCase):
    def assertFails(self, phase, code, text, **kwargs):
        with self.assertRaises(worker.BlockError) as caught:
            self.run_block(code, **kwargs)
        self.assertEqual(caught.exception.phase, phase)
        self.assertIn(text, str(caught.exception))
        return caught.exception

    def test_errors_name_their_phase(self):
        self.assertFails('load', 'def broken(:\n', 'SyntaxError')
        self.assertFails('load', '''
            def f():
                return 1
        ''', 'no function decorated with @transformer')
        error = self.assertFails('run', '''
            @transformer
            def f():
                raise ValueError('bad input')
        ''', 'ValueError: bad input')
        # The traceback starts at the block's code, not at the worker.
        self.assertIn('block.py', error.details)
        self.assertNotIn('worker.py', error.details)

    def test_failed_assertions_in_tests_fail_the_block(self):
        self.assertFails('test', '''
            @transformer
            def f():
                return 1

            @test
            def positive(output):
                assert output > 1, 'not positive'
        ''', '1 of 1 tests failed: positive')
        result = self.run_block('''
            @transformer
            def f():
                return 2

            @test
            def positive(output, *args):
                assert output > 1
        ''')
        self.assertEqual(result['tests'], [dict(name='positive', passed=True)])

    def test_unsupported_block_types_are_named(self):
        self.assertFails('load', '''
            @sensor
            def f():
                return True
        ''', 'sensor blocks are not supported', block_type='sensor')


class ProtocolTest(WorkerTestCase):
    def test_requests_get_one_reply_each_and_a_failure_does_not_stop_the_worker(self):
        good = self.block('''
            @transformer
            def f():
                return 1
        ''', name='good')
        bad = self.block('''
            @transformer
            def f():
                raise RuntimeError('boom')
        ''', name='bad')
        out = os.path.join(self.directory, 'out')
        requests = io.StringIO('\n'.join(json.dumps(r) for r in [
            dict(op='run', id='1', block=good, inputs=[], output_dir=out),
            dict(op='run', id='2', block=bad, inputs=[], output_dir=out),
            dict(op='run', id='3', block=good, inputs=[], output_dir=out),
            dict(op='shutdown', id='4'),
        ]) + '\n')
        channel = io.StringIO()
        worker.serve(requests, channel)
        replies = [json.loads(line) for line in channel.getvalue().splitlines()]
        self.assertEqual(
            [r['type'] for r in replies], ['ready', 'result', 'result', 'result', 'bye'],
        )
        self.assertEqual([r.get('ok') for r in replies[1:4]], [True, False, True])
        self.assertEqual(replies[2]['error']['phase'], 'run')


class ChainTest(WorkerTestCase):
    """Consecutive blocks in one worker request, outputs passing in memory."""

    def chain(self, codes, **extra):
        blocks = [self.block(code, name=f'b{i}') for i, code in enumerate(codes)]
        request = dict(
            block=blocks[0], chain=blocks, inputs=[], kwargs={},
            output_dir=os.path.join(self.directory, 'chain'), **extra,
        )
        return worker.run_block(request)

    FIRST = '''
        import pandas as pd

        @transformer
        def first(*args, **kwargs):
            return pd.DataFrame({'a': [1, 2, 3]})
    '''
    SECOND = '''
        @transformer
        def second(frame, *args, **kwargs):
            return frame.assign(b=frame['a'] * 10)

        @test
        def has_b(output, *args):
            assert 'b' in output.columns
    '''

    def test_outputs_pass_in_memory_and_only_the_last_is_written(self):
        result = self.chain([self.FIRST, self.SECOND])
        self.assertEqual([b['block_uuid'] for b in result['blocks']], ['b0', 'b1'])
        self.assertEqual(len(result['outputs']), 1)
        self.assertEqual(pl.read_ipc(result['outputs'][0]['path'])['b'].to_list(), [10, 20, 30])
        self.assertEqual(result['tests'], [dict(name='b1: has_b', passed=True)])
        self.assertNotIn('outputs', result['blocks'][0])
        self.assertFalse(os.path.exists(os.path.join(self.directory, 'chain', 'kept')))

    def test_every_output_is_kept_when_asked(self):
        result = self.chain([self.FIRST, self.SECOND], keep_outputs=True)
        kept = result['blocks'][0]['outputs']
        self.assertEqual(pl.read_ipc(kept[0]['path'])['a'].to_list(), [1, 2, 3])

    def test_blocks_get_what_the_file_between_blocks_gives(self):
        # An object column of strings becomes the str dtype through the file; a chain
        # must give the next block that too, not the object column in memory.
        frame = pd.DataFrame({'text': pd.Series(['x', 'y'], dtype=object), 'n': [1, 2]})
        through_file = worker.read_input(self.write(frame))
        chained = worker._as_exchanged([frame], os.path.join(self.directory, 'x'))[0]
        pd.testing.assert_frame_equal(chained, through_file)
        self.assertNotEqual(chained['text'].dtype, np.dtype('O'))
        # A Polars frame gives the same value from memory, so it is not copied.
        polars_frame = pl.DataFrame({'a': [1]})
        self.assertIs(worker._as_exchanged([polars_frame], self.directory)[0], polars_frame)

    def test_a_failure_names_its_block(self):
        with self.assertRaises(worker.BlockError) as caught:
            self.chain([self.FIRST, '''
                @transformer
                def second(frame, *args, **kwargs):
                    raise ValueError('bad')
            '''])
        self.assertEqual((caught.exception.phase, caught.exception.block_uuid), ('run', 'b1'))
        self.assertIn('ValueError: bad', str(caught.exception))

        with self.assertRaises(worker.BlockError) as caught:
            self.chain(['''
                @transformer
                def first(*args, **kwargs):
                    return 1

                @test
                def big(output, *args):
                    assert output > 5, 'too small'
            ''', self.SECOND])
        self.assertEqual((caught.exception.phase, caught.exception.block_uuid), ('test', 'b0'))


class StreamBatchTest(WorkerTestCase):
    """A stream's transformers run on each batch of messages in a resident worker."""

    def setUp(self):
        super().setUp()
        worker._STREAM_FUNCTIONS.clear()

    def stream(self, chain, messages, kwargs=None):
        return worker.run_stream_batch(dict(chain=chain, messages=messages, kwargs=kwargs or {}))

    def test_each_block_gets_the_previous_blocks_messages(self):
        double = self.block('''
            @transformer
            def f(messages, *args, **kwargs):
                return [dict(m, n=m['n'] * 2) for m in messages]
        ''', name='double')
        keep_big = self.block('''
            import pandas as pd

            @transformer
            def f(messages, **kwargs):
                frame = pd.DataFrame(messages)
                return frame[frame['n'] > kwargs['minimum']]
        ''', name='keep_big')

        result = self.stream([double, keep_big], [{'n': 1}, {'n': 5}], dict(minimum=4))

        self.assertEqual(result['messages'], [{'n': 10}])
        self.assertEqual([b['block_uuid'] for b in result['blocks']], ['double', 'keep_big'])

    def test_top_level_code_runs_once_per_worker(self):
        """Loading a model at the top of a block ran for every batch."""
        counter = os.path.join(self.directory, 'loads')
        block = self.block(f'''
            with open({counter!r}, 'a') as file:
                file.write('x')

            @transformer
            def f(messages):
                return messages
        ''', name='model')

        for _ in range(3):
            self.stream([block], [{'n': 1}])

        with open(counter) as file:
            self.assertEqual(file.read(), 'x')

    def test_polars_none_and_one_message_results(self):
        to_polars = self.block('''
            import polars as pl

            @transformer
            def f(messages):
                return pl.DataFrame(messages).with_columns(m=pl.col('n') + 1)
        ''', name='to_polars')
        one = self.block('''
            @transformer
            def f(messages):
                return {'count': len(messages)}
        ''', name='one')
        nothing = self.block('''
            @transformer
            def f(messages):
                return None
        ''', name='nothing')

        self.assertEqual(self.stream([to_polars], [{'n': 1}])['messages'], [{'n': 1, 'm': 2}])
        self.assertEqual(self.stream([one], [{}, {}])['messages'], [{'count': 2}])
        self.assertEqual(self.stream([nothing, one], [{}])['messages'], [])

    def test_nan_fails_the_block_with_the_field(self):
        """NaN and infinity have no JSON form; they became null without a word."""
        block = self.block('''
            @transformer
            def f(messages):
                return [{'ok': 1.5, 'score': float('nan')}]
        ''', name='nan')

        with self.assertRaises(worker.BlockError) as raised:
            self.stream([block], [{}])

        self.assertIn('messages[0].score', str(raised.exception))
        self.assertEqual(raised.exception.block_uuid, 'nan')

    def test_nan_between_blocks_is_allowed_and_numpy_nan_is_named(self):
        """Only messages leaving the worker are JSON; a block may hand NaN to the next."""
        scores = self.block('''
            @transformer
            def f(messages):
                return [{'score': float('nan')}, {'score': 2.0}]
        ''', name='scores')
        keep = self.block('''
            import math

            @transformer
            def f(messages):
                return [m for m in messages if not math.isnan(m['score'])]
        ''', name='keep')
        numpy_nan = self.block('''
            import numpy as np

            @transformer
            def f(messages):
                return [{'score': np.float32('nan')}]
        ''', name='numpy_nan')

        self.assertEqual(self.stream([scores, keep], [{}])['outputs'], {'keep': [{'score': 2.0}]})
        with self.assertRaises(worker.BlockError) as raised:
            self.stream([numpy_nan], [{}])
        self.assertEqual((raised.exception.phase, raised.exception.block_uuid),
                         ('run', 'numpy_nan'))

    def test_a_message_that_is_not_json_names_its_block(self):
        """A RabbitMQ Payload returned as it came was a worker error naming no block."""
        returns_object = self.block('''
            from types import SimpleNamespace

            @transformer
            def f(messages):
                return [SimpleNamespace(body=b'{}')]
        ''', name='returns_object')

        with self.assertRaises(worker.BlockError) as raised:
            self.stream([returns_object], [{}])

        self.assertEqual((raised.exception.phase, raised.exception.block_uuid),
                         ('run', 'returns_object'))
        self.assertIn('SimpleNamespace is not JSON serializable', str(raised.exception))

    def test_a_block_that_takes_a_dataframe_gets_one_and_returns_one(self):
        """Annotated with pl.DataFrame or pd.DataFrame, a block gets its messages as a table."""
        frame = self.block('''
            import polars as pl

            @transformer
            def f(messages: pl.DataFrame, *args, **kwargs):
                assert isinstance(messages, pl.DataFrame), type(messages)
                return messages.with_columns(double=pl.col('n') * 2)
        ''', name='frame')
        pandas = self.block('''
            import pandas as pd

            @transformer
            def f(messages: pd.DataFrame, *args, **kwargs):
                assert isinstance(messages, pd.DataFrame), type(messages)
                return messages.assign(seen=True)
        ''', name='pandas')

        result = self.stream([frame, pandas], [{'n': 1}, {'n': 2}])

        self.assertEqual(result['messages'], [{'n': 1, 'double': 2, 'seen': True},
                                              {'n': 2, 'double': 4, 'seen': True}])

    def test_nan_in_a_returned_dataframe_names_its_row_and_column(self):
        """Polars writes NaN as null: it is refused, as in a list of messages."""
        block = self.block('''
            import polars as pl

            @transformer
            def f(messages: pl.DataFrame, *args, **kwargs):
                return messages.with_columns(score=pl.Series([1.0, float('nan')]))
        ''', name='nan_frame')

        with self.assertRaises(worker.BlockError) as raised:
            worker._stream_batch(dict(chain=[block], messages=[{'n': 1}, {'n': 2}], kwargs={}))

        self.assertIn('messages[1].score', str(raised.exception))

    def test_a_batch_file_is_read_as_a_table_or_message_by_message(self):
        """The engine writes JSON lines for blocks that take a DataFrame."""
        block = self.block('''
            import polars as pl

            @transformer
            def f(messages: pl.DataFrame, *args, **kwargs):
                return messages
        ''', name='file_frame')
        path = os.path.join(self.directory, 'batch.ndjson')
        with open(path, 'w') as file:
            file.write('{"n": 1}\n{"n": 2}\n')

        result = worker.run_stream_batch(dict(chain=[block], messages=[], messages_file=path,
                                              count=2, kwargs={}))
        self.assertEqual(result['messages'], [{'n': 1}, {'n': 2}])

        with open(path, 'w') as file:
            file.write('{"n": 1}\n"plain text"\n')
        with self.assertRaises(worker.BlockError) as raised:
            worker.run_stream_batch(dict(chain=[block], messages=[], messages_file=path,
                                         count=2, kwargs={}))
        self.assertIn('must be JSON objects', str(raised.exception))
        self.assertEqual(raised.exception.block_uuid, 'file_frame')

    def test_stream_load_says_what_each_block_takes(self):
        frame = self.block('''
            import polars as pl

            @transformer
            def f(messages: pl.DataFrame, *args, **kwargs):
                return messages
        ''', name='takes_frame')
        plain = self.block('''
            @transformer
            def f(messages, *args, **kwargs):
                return messages
        ''', name='takes_list')
        requests = io.StringIO(json.dumps(dict(id='1', op='stream_load',
                                               chain=[frame, plain])) + '\n')
        channel = io.StringIO()

        worker.serve(requests, channel)

        reply = [json.loads(line) for line in channel.getvalue().splitlines()][-1]
        self.assertEqual(reply['inputs'], {'takes_frame': 'polars', 'takes_list': 'messages'})

    def test_event_hubs_messages_reach_the_block_as_mage_hands_them(self):
        block = self.block('''
            @transformer
            def f(messages, *args, **kwargs):
                return [dict(seen=m['data']) for m in messages]
        ''', name='hubs')

        result = worker.run_stream_batch(dict(chain=[block], messages=['{"a": 1}', 'text'],
                                              message_shape='event_hub', kwargs={}))

        self.assertEqual(result['messages'], [dict(seen='{"a": 1}'), dict(seen='text')])

    def test_raw_messages_reach_the_block_as_bytes(self):
        """Mage's RAW_VALUE: the engine sends each message's bytes as base64."""
        block = self.block('''
            @transformer
            def f(messages, *args, **kwargs):
                return [dict(kind=type(m).__name__, size=len(m)) for m in messages]
        ''', name='raw')

        result = worker.run_stream_batch(dict(chain=[block], messages=['AP8=', ''],
                                              message_shape='raw', kwargs={}))

        self.assertEqual(result['messages'], [dict(kind='bytes', size=2),
                                              dict(kind='bytes', size=0)])

    def test_a_failing_block_is_named(self):
        fails = self.block('''
            @transformer
            def f(messages):
                raise ValueError('bad message')
        ''', name='fails')

        with self.assertRaises(worker.BlockError) as raised:
            self.stream([fails], [{}])

        self.assertEqual(raised.exception.block_uuid, 'fails')
        self.assertEqual(raised.exception.phase, 'run')
        self.assertIn('bad message', str(raised.exception))

    def test_the_serve_loop_routes_stream_requests(self):
        block = self.block('''
            @transformer
            def f(messages):
                return [{'seen': len(messages)}]
        ''', name='seen')
        requests = io.StringIO(json.dumps(dict(
            id='1', op='stream', chain=[block], messages=[1, 2, 3],
        )) + '\n')
        channel = io.StringIO()

        worker.serve(requests, channel)

        replies = [json.loads(line) for line in channel.getvalue().splitlines()]
        self.assertEqual(replies[-1]['outputs'], {'seen': [{'seen': 3}]})
        self.assertNotIn('messages', replies[-1])
        self.assertTrue(replies[-1]['ok'])
        self.assertIsInstance(replies[-1]['rss_bytes'], int)


class StreamIsolationTest(WorkerTestCase):
    """
    With a dead-letter sink, the messages that fail on their own are found and the others
    flow; a code problem (every message failing, a block that does not load) fails the batch.
    """

    def setUp(self):
        super().setUp()
        worker._STREAM_FUNCTIONS.clear()
        self.inverse = self.block('''
            @transformer
            def f(messages):
                return [dict(m, inverse=1 / m['n']) for m in messages]
        ''', name='inverse')

    def stream(self, messages, isolate=True, chain=None):
        return worker.run_stream_batch(dict(
            chain=chain or [self.inverse], messages=messages, isolate=isolate,
        ))

    def test_the_messages_that_fail_on_their_own_are_found_and_the_others_flow(self):
        messages = [{'n': n} for n in (1, 0, 2, 4, 0, 5, 8)]

        result = self.stream(messages)

        self.assertEqual([f['row'] for f in result['failures']], [1, 4])
        self.assertIn('ZeroDivisionError', result['failures'][0]['message'])
        self.assertEqual(result['failures'][0]['block_uuid'], 'inverse')
        self.assertEqual([m['n'] for m in result['outputs']['inverse']], [1, 2, 4, 5, 8])

    def test_every_message_failing_is_a_failure_of_the_batch(self):
        with self.assertRaises(worker.BlockError):
            self.stream([{'n': 0}, {'n': 0}])

    def test_without_isolation_one_message_fails_the_batch(self):
        with self.assertRaises(worker.BlockError):
            self.stream([{'n': 1}, {'n': 0}], isolate=False)

    def test_a_block_that_does_not_load_fails_the_batch(self):
        broken = self.block('this is not python', name='broken')
        with self.assertRaises(worker.BlockError) as caught:
            self.stream([{'n': 1}], chain=[broken])
        self.assertEqual(caught.exception.phase, 'load')


class RabbitmqPayloadTest(WorkerTestCase):
    """A RabbitMQ stream's blocks get Mage's Payload(method, properties, body)."""

    def test_blocks_written_for_mages_rabbitmq_source_run_unchanged(self):
        worker._STREAM_FUNCTIONS.clear()
        block = self.block('''
            import json

            @transformer
            def f(messages):
                return [dict(json.loads(m.body), tag=m.method.delivery_tag,
                             trace=m.properties.headers.get('trace')) for m in messages]
        ''', name='rabbit')

        result = worker.run_stream_batch(dict(
            chain=[block], message_shape='rabbitmq',
            messages=[{'body': '{"id": 1}', 'delivery_tag': 7, 'headers': {'trace': 'x'}},
                      {'body_base64': 'eyJpZCI6IDJ9', 'delivery_tag': 8}],
        ))

        self.assertEqual(result['messages'], [
            dict(id=1, tag=7, trace='x'), dict(id=2, tag=8, trace=None),
        ])


class StreamLoadTest(WorkerTestCase):
    """
    A replaced worker gets a stream's blocks loaded before the stream's next batch, so a model
    loaded at the top of a block does not hold that batch up; every reply carries the worker's
    memory, which the service replaces stream workers on.
    """

    def setUp(self):
        super().setUp()
        worker._STREAM_FUNCTIONS.clear()

    def test_stream_load_runs_top_level_code_once_without_a_batch(self):
        marker = os.path.join(self.directory, 'loads')
        block = self.block(f'''
            with open({marker!r}, 'a') as file:
                file.write('x')

            @transformer
            def f(messages):
                return messages
        ''', name='model')
        requests = io.StringIO('\n'.join(json.dumps(r) for r in [
            dict(id='1', op='stream_load', chain=[block]),
            dict(id='2', op='stream', chain=[block], messages=[1]),
        ]) + '\n')
        channel = io.StringIO()

        worker.serve(requests, channel)

        replies = [json.loads(line) for line in channel.getvalue().splitlines()][1:]
        self.assertEqual(replies[0]['loaded'], ['model'])
        self.assertEqual(replies[1]['outputs'], {'model': [1]})
        with open(marker) as file:
            self.assertEqual(file.read(), 'x')
        self.assertTrue(all(r['rss_bytes'] > 0 for r in replies))


class StreamBranchesTest(WorkerTestCase):
    """Streams branch: sinks follow different blocks, each fed its upstream's messages."""

    def setUp(self):
        super().setUp()
        worker._STREAM_FUNCTIONS.clear()

    def test_each_branch_gets_its_upstreams_messages(self):
        clean = dict(self.block('''
            @transformer
            def f(messages):
                return [m for m in messages if m['n'] >= 0]
        ''', name='clean'), upstream=[])
        double = dict(self.block('''
            @transformer
            def f(messages):
                return [dict(m, n=m['n'] * 2) for m in messages]
        ''', name='double'), upstream=['clean'])
        count = dict(self.block('''
            @transformer
            def f(messages):
                return {'count': len(messages)}
        ''', name='count'), upstream=['clean'])

        result = worker.run_stream_batch(dict(
            chain=[clean, double, count], messages=[{'n': -1}, {'n': 2}, {'n': 3}],
            outputs=['double', 'count'], kwargs={},
        ))

        self.assertEqual(result['outputs'], {
            'double': [{'n': 4}, {'n': 6}],
            'count': [{'count': 2}],
        })

    def test_a_block_after_an_empty_output_does_not_run_and_joins_are_refused(self):
        nothing = dict(self.block('''
            @transformer
            def f(messages):
                return []
        ''', name='nothing'), upstream=[])
        boom = dict(self.block('''
            @transformer
            def f(messages):
                raise RuntimeError('should not run')
        ''', name='boom'), upstream=['nothing'])
        result = worker.run_stream_batch(dict(
            chain=[nothing, boom], messages=[{}], outputs=['boom'], kwargs={},
        ))
        self.assertEqual(result['outputs'], {'boom': []})

        join = dict(boom, upstream=['nothing', 'other'])
        with self.assertRaisesRegex(worker.BlockError, 'reads 2 blocks'):
            worker.run_stream_batch(dict(chain=[nothing, join], messages=[{}], kwargs={}))


class StreamStateTest(WorkerTestCase):
    """
    Keyed state in stream blocks: the engine sends the values of the batch's keys, the block
    changes them, and only the changes go back; a message that fails changes nothing.
    """

    def setUp(self):
        super().setUp()
        worker._STREAM_FUNCTIONS.clear()
        self.count = self.block('''
            @transformer
            def f(messages, *args, **kwargs):
                state = kwargs['state']
                out = []
                for m in messages:
                    seen = state.get(m['card'], 0) + 1
                    if m.get('fail'):
                        raise ValueError('a bad message')
                    state[m['card']] = seen
                    out.append(dict(m, seen=seen))
                if 'forget' in state:
                    del state['forget']
                return out
        ''', name='count')

    def stream(self, messages, state, isolate=False):
        return worker.run_stream_batch(dict(
            chain=[self.count], messages=messages, isolate=isolate, kwargs={},
            state={'count': state},
        ))

    def test_a_block_reads_and_changes_its_keys_state(self):
        result = self.stream([{'card': 'a'}, {'card': 'a'}, {'card': 'b'}],
                             {'a': 5, 'c': 1, 'forget': 1})
        self.assertEqual([m['seen'] for m in result['outputs']['count']], [6, 7, 1])
        self.assertEqual(result['state_updates'], {'count': {'a': 7, 'b': 1, 'forget': None}})

    def test_a_message_that_fails_on_its_own_leaves_the_state_as_it_was(self):
        result = self.stream([{'card': 'a'}, {'card': 'b', 'fail': True}, {'card': 'a'}],
                             {'a': 1}, isolate=True)
        self.assertEqual([f['row'] for f in result['failures']], [1])
        # a was counted twice, by the messages that ran; b's failed message left no trace.
        self.assertEqual(result['state_updates'], {'count': {'a': 3}})

    def test_without_state_there_are_no_state_updates(self):
        plain = self.block('''
            @transformer
            def f(messages, *args, **kwargs):
                return messages
        ''', name='plain')
        result = worker.run_stream_batch(dict(chain=[plain], messages=[{'n': 1}], kwargs={}))
        self.assertNotIn('state_updates', result)
