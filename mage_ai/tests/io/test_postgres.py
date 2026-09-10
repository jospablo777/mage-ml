import unittest
from unittest.mock import patch

from psycopg2 import OperationalError

from mage_ai.io.postgres import Postgres


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
