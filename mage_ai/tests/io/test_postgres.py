import io
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
from psycopg2 import OperationalError

from mage_ai.io.constants import (
    UNIQUE_CONFLICT_METHOD_IGNORE,
    UNIQUE_CONFLICT_METHOD_UPDATE,
)
from mage_ai.io.export_utils import infer_dtypes
from mage_ai.io.postgres import INSERT_PAGE_SIZE, Postgres


class PostgresConnectionTest(unittest.TestCase):
    def loader(self, **kwargs):
        return Postgres(
            dbname='test', user='mage', password='password', host='db.internal',
            verbose=False, **kwargs,
        )

    @patch('mage_ai.io.postgres.connect', side_effect=OperationalError('connection failed'))
    def test_connection_failure_propagates(self, connect):
        with self.assertRaisesRegex(OperationalError, 'connection failed'):
            self.loader().open()

    @patch('mage_ai.io.postgres.connect')
    def test_direct_connection_preserves_libpq_port_options(self, connect):
        for port in (None, '5432,5433'):
            with self.subTest(port=port):
                loader = self.loader(port=port)
                loader.open()
                self.assertEqual(connect.call_args.kwargs['port'], port)
                loader.close()

    @patch('mage_ai.io.postgres.SSHTunnelForwarder')
    @patch('mage_ai.io.postgres.connect')
    def test_tunnel_uses_loopback_and_an_assigned_port(self, connect, forwarder):
        forwarder.return_value.local_bind_port = 15432
        loader = self.loader(connection_method='ssh_tunnel', port='5432', ssh_port='22')

        loader.open()

        self.assertEqual(forwarder.call_args.kwargs['local_bind_address'], ('127.0.0.1', 0))
        self.assertEqual(forwarder.call_args.kwargs['remote_bind_address'], ('db.internal', 5432))
        self.assertEqual(connect.call_args.kwargs['host'], '127.0.0.1')
        self.assertEqual(connect.call_args.kwargs['port'], 15432)
        loader.close()
        forwarder.return_value.stop.assert_called_once()
        connect.return_value.close.assert_called_once()

    @patch('mage_ai.io.postgres.SSHTunnelForwarder')
    @patch('mage_ai.io.postgres.connect', side_effect=OperationalError('connection failed'))
    def test_connection_failure_stops_tunnel(self, connect, forwarder):
        loader = self.loader(connection_method='ssh_tunnel')

        with self.assertRaises(OperationalError):
            loader.open()

        forwarder.return_value.stop.assert_called_once()
        self.assertIsNone(loader.ssh_tunnel)

    @patch('mage_ai.io.postgres.SSHTunnelForwarder')
    @patch('mage_ai.io.postgres.connect')
    def test_tunnel_failure_stops_tunnel(self, connect, forwarder):
        forwarder.return_value.start.side_effect = RuntimeError('tunnel failed')
        loader = self.loader(connection_method='ssh_tunnel')

        with self.assertRaisesRegex(RuntimeError, 'tunnel failed'):
            loader.open()

        connect.assert_not_called()
        forwarder.return_value.stop.assert_called_once()
        self.assertIsNone(loader.ssh_tunnel)


class NumpyAdapterTest(unittest.TestCase):
    """
    psycopg2 rejects numpy integers and booleans, and numpy floats reached the float
    adapter through inheritance, where numpy 2's repr produced "np.float64(0.5)".
    """

    def quoted(self, value):
        from psycopg2.extensions import adapt

        return adapt(value).getquoted()

    def test_numpy_integers_bind_as_numbers(self):
        for value in (np.int8(3), np.int16(3), np.int32(3), np.int64(345100), np.uint8(3)):
            with self.subTest(dtype=type(value).__name__):
                self.assertEqual(self.quoted(value), str(int(value)).encode())

    def test_numpy_booleans_bind_as_booleans(self):
        self.assertEqual(self.quoted(np.bool_(True)), b'true')
        self.assertEqual(self.quoted(np.bool_(False)), b'false')

    def test_numpy_floats_bind_as_numbers(self):
        self.assertEqual(self.quoted(np.float64(0.5)), b'0.5')
        self.assertEqual(self.quoted(np.float32(0.5)), b'0.5')

    def test_numpy_nan_uses_the_postgres_literal(self):
        self.assertEqual(self.quoted(np.float64('nan')), b"'NaN'::float")


class FakeCursor:
    def __init__(self):
        self.copy_calls = []

    def copy_expert(self, query, buffer):
        self.copy_calls.append((query, buffer.getvalue()))


class UploadDataframeTest(unittest.TestCase):
    """What the exporter hands to psycopg2 for a validated feature batch."""

    def setUp(self):
        self.loader = Postgres(
            dbname='test', user='mage', password='password', host='db.internal', verbose=False,
        )
        self.df = pd.DataFrame({
            'case_id': pd.Series([345038, 345039], dtype='int64'),
            'opened_at': pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'] * 2, utc=True),
            'flag': pd.Series([True, False], dtype='bool'),
            'total_order': pd.Series([0, 1], dtype='int16'),
            'note': pd.Series(['first', None]),
        })
        self.dtypes = infer_dtypes(self.df)

    def upload(self, cursor, **kwargs):
        self.loader.upload_dataframe(
            cursor, self.df, {}, self.dtypes, 'feature_store.case_features', **kwargs,
        )

    def insert(self, **kwargs):
        """Run the insert path with execute_values captured."""
        options = dict(
            unique_constraints=['case_id', 'opened_at'],
            unique_conflict_method=UNIQUE_CONFLICT_METHOD_IGNORE,
        )
        options.update(kwargs)

        with patch('mage_ai.io.postgres.execute_values') as execute:
            self.upload(FakeCursor(), **options)

        return execute.call_args

    def test_rows_go_out_in_pages_rather_than_one_statement_each(self):
        """
        psycopg2's executemany sends one statement per row, so a batch costs one
        network round trip per row. execute_values sends a page of rows in one.
        """
        call = self.insert()

        self.assertIn('VALUES %s', call.args[1])
        self.assertEqual(call.kwargs['page_size'], INSERT_PAGE_SIZE)
        self.assertEqual(len(call.args[2]), 2)

    def test_the_page_size_is_configurable(self):
        self.assertEqual(self.insert(insert_page_size=25).kwargs['page_size'], 25)

    def test_conflict_clause_uses_the_unique_constraint(self):
        query = self.insert().args[1]

        self.assertIn('ON CONFLICT ("case_id", "opened_at")', query)
        self.assertIn('DO NOTHING', query)

    def test_conflict_update_assigns_every_column(self):
        query = self.insert(unique_conflict_method=UNIQUE_CONFLICT_METHOD_UPDATE).args[1]

        self.assertIn('DO UPDATE SET', query)
        self.assertIn('"note" = EXCLUDED."note"', query)

    def test_every_bound_value_is_adaptable(self):
        from psycopg2.extensions import adapt

        for row in self.insert().args[2]:
            for value in row:
                if value is None:
                    continue
                adapt(value).getquoted()

    def test_missing_values_become_null(self):
        self.assertIsNone(self.insert().args[2][1][-1])

    def test_copy_path_writes_microsecond_timestamps(self):
        cursor = FakeCursor()

        self.upload(cursor, buffer=io.StringIO())

        contents = cursor.copy_calls[0][1]
        self.assertIn('2026-07-20 18:15:34.258586+00:00', contents)
        # na_rep='' plus FORCE_NULL turns the empty field into NULL.
        self.assertTrue(contents.endswith(',False,1,\n'))
