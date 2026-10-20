"""
The Pinot client (mage_ai/io/pinot.py) without a server: frames typed from Pinot's column
types, the LIMIT it adds, its io_config settings and its connection.
"""
import datetime as dt
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.pinot import Pinot, pinot_frame
from mage_ai.tests.base_test import TestCase


class PinotFrameTest(TestCase):
    def test_columns_take_pinot_types_and_keep_nulls(self):
        schema = [
            dict(name='id', type='INT'), dict(name='big', type='LONG'),
            dict(name='ratio', type='DOUBLE'), dict(name='score', type='FLOAT'),
            dict(name='flag', type='BOOLEAN'), dict(name='at', type='TIMESTAMP'),
            dict(name='amount', type='BIG_DECIMAL'), dict(name='raw', type='BYTES'),
            dict(name='doc', type='JSON'), dict(name='tags', type='STRING_ARRAY'),
            dict(name='name', type='STRING'),
        ]
        rows = [
            [1, 2 ** 53 + 1, 0.1, 1.5, True, dt.datetime(2024, 6, 1, 10, 0, 0, 123000),
             '"12345678901234567890.123456789"', '00ff', {'k': 1}, ['a'], 'Zürich'],
            [None, None, None, None, None, None, None, None, None, None, None],
        ]
        frame = pinot_frame(rows, schema)

        self.assertEqual(
            {c: str(t) for c, t in frame.dtypes.items()},
            dict(id='Int32', big='Int64', ratio='float64', score='float32', flag='boolean',
                 at='datetime64[ms, UTC]', amount='object', raw='object', doc='object',
                 tags='object', name='object'),
        )
        first = frame.iloc[0]
        self.assertEqual(first['big'], 2 ** 53 + 1)
        self.assertEqual(first['amount'], Decimal('12345678901234567890.123456789'))
        self.assertEqual(first['raw'], b'\x00\xff')
        self.assertEqual(
            first['at'], dt.datetime(2024, 6, 1, 10, 0, 0, 123000, tzinfo=dt.timezone.utc),
        )
        self.assertTrue(frame.iloc[1][['id', 'big', 'flag', 'at']].isna().all())
        self.assertIsNone(frame.iloc[1]['amount'])

    def test_no_rows_keep_the_columns(self):
        frame = pinot_frame([], [dict(name='id', type='INT'), dict(name='n', type='STRING')])
        self.assertEqual(list(frame.columns), ['id', 'n'])
        self.assertEqual(len(frame), 0)


class PinotClientTest(TestCase):
    def test_a_limit_is_added_unless_the_query_ends_with_one(self):
        pinot = Pinot(host='localhost', port=8099, verbose=False)
        self.assertTrue(
            pinot._enforce_limit('SELECT credit_limit FROM t', 50).endswith('LIMIT 50'),
        )
        for query in ('SELECT * FROM t LIMIT 5', 'select * from t limit 10, 20;',
                      'SELECT * FROM t LIMIT 5 OFFSET 10'):
            self.assertEqual(pinot._enforce_limit(query, 50), query.rstrip(';'))

    def test_queries_ask_for_null_handling_and_open_on_first_use(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [[1]]
        cursor.schema = [dict(name='id', type='INT')]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        with patch('mage_ai.io.pinot.connect', return_value=connection) as connect:
            pinot = Pinot(host='localhost', port=8099, verbose=False)
            frame = pinot.load('SELECT id FROM t')
        connect.assert_called_once()
        query, = cursor.execute.call_args.args
        # One row over the limit, to tell a cut result (checks_row_limit).
        self.assertRegex(query, r'LIMIT \d+$')
        self.assertEqual(cursor.execute.call_args.kwargs['queryOptions'],
                         'enableNullHandling=true')
        self.assertEqual(frame['id'].tolist(), [1])

    def test_a_profile_without_path_or_scheme_gets_the_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'io_config.yaml'
            path.write_text('default:\n  PINOT_HOST: broker\n  PINOT_PORT: 8099\n')
            pinot = Pinot.with_config(ConfigFileLoader(str(path), 'default'))
            verbose = Path(directory) / 'verbose.yaml'
            verbose.write_text(
                'default:\n  Pinot:\n    host: broker\n    user: me\n    password: secret\n',
            )
            config = ConfigFileLoader(str(verbose), 'default')
        self.assertEqual(pinot.settings['path'], '/query/sql')
        self.assertEqual(pinot.settings['scheme'], 'http')
        # The verbose format mapped PINOT_USER to password and had no PINOT_PASSWORD.
        self.assertEqual(config['PINOT_USER'], 'me')
        self.assertEqual(config['PINOT_PASSWORD'], 'secret')
