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
